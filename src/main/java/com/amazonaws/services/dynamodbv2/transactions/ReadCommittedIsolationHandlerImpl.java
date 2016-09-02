/**
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.transactions;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionNotFoundException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.UnknownCompletedTransactionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.transactions.Transaction.getOwner;
import static com.amazonaws.services.dynamodbv2.transactions.Transaction.isApplied;
import static com.amazonaws.services.dynamodbv2.transactions.Transaction.isTransient;
import static com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException.txAssert;

/**
 * An isolation handler for reading items at the committed
 * (Transaction.IsolationLevel.COMMITTED) level. It will
 * filter out transient items. If there is an applied, but
 * uncommitted item, the isolation handler will attempt to
 * get the last committed version of the item.
 */
public class ReadCommittedIsolationHandlerImpl implements ReadIsolationHandler {

    private static final int DEFAULT_NUM_RETRIES = 2;
    private static final Log LOG = LogFactory.getLog(ReadCommittedIsolationHandlerImpl.class);

    private final TransactionManager txManager;
    private final int numRetries;

    public ReadCommittedIsolationHandlerImpl(final TransactionManager txManager) {
        this(txManager, DEFAULT_NUM_RETRIES);
    }

    public ReadCommittedIsolationHandlerImpl(final TransactionManager txManager, final int numRetries) {
        this.txManager = txManager;
        this.numRetries = numRetries;
    }

    /**
     * Return the item that's passed in if it's not locked. Otherwise, throw a TransactionException.
     * @param item The item to check
     * @return The item if it's locked (or if it's locked, but not yet applied)
     */
    protected Map<String, AttributeValue> checkItemCommitted(final Map<String, AttributeValue> item) {
        // If the item doesn't exist, it's not locked
        if (item == null) {
            return null;
        }
        // If the item is transient, return null
        if (isTransient(item)) {
            return null;
        }
        // If the item isn't applied, it doesn't matter if it's locked
        if (!isApplied(item)) {
            return item;
        }
        // If the item isn't locked, return it
        String lockingTxId = getOwner(item);
        if (lockingTxId == null) {
            return item;
        }

        throw new TransactionException(lockingTxId, "Item has been modified in an uncommitted transaction.");
    }

    /**
     * Get an old committed version of an item from the images table.
     * @param lockingTx The transaction that is currently locking the item.
     * @param tableName The table that contains the item
     * @param key The item's key
     * @return a previously committed version of the item
     */
    protected Map<String, AttributeValue> getOldCommittedItem(
            final Transaction lockingTx,
            final String tableName,
            final Map<String, AttributeValue> key) {
        Request lockingRequest = lockingTx.getTxItem().getRequestForKey(tableName, key);
        txAssert(lockingRequest != null, null, "Expected transaction to be locking request, but no request found for tx", lockingTx.getId(), "table", tableName, "key ", key);
        Map<String, AttributeValue> oldItem = lockingTx.getTxItem().loadItemImage(lockingRequest.getRid());
        if (oldItem == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Item image " + lockingRequest.getRid() + " missing for transaction " + lockingTx.getId());
            }
            throw new UnknownCompletedTransactionException(
                    lockingTx.getId(),
                    "Transaction must have completed since the old copy of the image is missing");
        }
        return oldItem;
    }

    /**
     * Create a GetItemRequest for an item (in the event that you need to get the item again).
     * @param tableName The table that holds the item
     * @param item The item to get
     * @return the request
     */
    protected GetItemRequest createGetItemRequest(
            final String tableName,
            final Map<String, AttributeValue> item) {
        Map<String, AttributeValue> key = txManager.createKeyMap(tableName, item);

        /*
         * Set the request to consistent read the next time around, since we may have read while locking tx
         * was cleaning up or read a stale item that is no longer locked
         */
        GetItemRequest request = new GetItemRequest()
                .withTableName(tableName)
                .withKey(key)
                .withConsistentRead(true);
        return request;
    }

    protected Transaction loadTransaction(String txId) {
        return new Transaction(txId, txManager, false);
    }

    /**
     * Returns the item that's passed in if it's not locked. Otherwise, tries to get an old
     * committed version of the item. If that's not possible, it retries.
     * @param item The item to check.
     * @param tableName The table that contains the item
     * @return A committed version of the item (not necessarily the latest committed version).
     */
    protected Map<String, AttributeValue> handleItem(
            final Map<String, AttributeValue> item,
            final String tableName,
            final int numRetries) {
        GetItemRequest request = null; // only create if necessary
        for (int i = 0; i <= numRetries; i++) {
            final Map<String, AttributeValue> currentItem;
            if (i == 0) {
                currentItem = item;
            } else {
                if (request == null) {
                    request = createGetItemRequest(tableName, item);
                }
                currentItem = txManager.getClient().getItem(request).getItem();
            }

            // 1. Return the item if it isn't locked (or if it's locked, but not applied yet)
            try {
                return checkItemCommitted(currentItem);
            } catch (TransactionException e1) {
                try {
                    // 2. Load the locking transaction
                    Transaction lockingTx = loadTransaction(e1.getTxId());

                    /*
                     * 3. See if the locking transaction has been committed. If so, return the item. This is valid because you cannot
                     * write to an item multiple times in the same transaction. Otherwise it would expose intermediate state.
                     */
                    if (TransactionItem.State.COMMITTED.equals(lockingTx.getTxItem().getState())) {
                        return currentItem;
                    }

                    // 4. Try to get a previously committed version of the item
                    if (request == null) {
                        request = createGetItemRequest(tableName, item);
                    }
                    return getOldCommittedItem(lockingTx, tableName, request.getKey());
                } catch (UnknownCompletedTransactionException e2) {
                    LOG.debug("Could not find item image. Transaction must have already completed.", e2);
                } catch (TransactionNotFoundException e2) {
                    LOG.debug("Unable to find locking transaction. Transaction must have already completed.", e2);
                }
            }
        }
        throw new TransactionException(null, "Ran out of attempts to get a committed image of the item");
    }

    protected Map<String, AttributeValue> filterAttributesToGet(
            final Map<String, AttributeValue> item,
            final List<String> attributesToGet) {
        if (item == null) {
            return null;
        }
        if (attributesToGet == null || attributesToGet.isEmpty()) {
            return item;
        }
        Map<String, AttributeValue> result = new HashMap<String, AttributeValue>();
        for (String attributeName : attributesToGet) {
            AttributeValue value = item.get(attributeName);
            if (value != null) {
                result.put(attributeName, value);
            }
        }
        return result;
    }

    @Override
    public Map<String, AttributeValue> handleItem(
            final Map<String, AttributeValue> item,
            final List<String> attributesToGet,
            final String tableName) {
        return filterAttributesToGet(
                handleItem(item, tableName, numRetries),
                attributesToGet);
    }

}
