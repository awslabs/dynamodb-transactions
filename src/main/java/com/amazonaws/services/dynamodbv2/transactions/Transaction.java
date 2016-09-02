/**
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.transactions.Request.DeleteItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.GetItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.PutItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.UpdateItem;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.DuplicateRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.ItemNotLockedException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionCommittedException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionCompletedException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionNotFoundException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionRolledBackException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.UnknownCompletedTransactionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import static com.amazonaws.services.dynamodbv2.transactions.TransactionItem.State;
import static com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException.txAssert;

/**
 * A transaction that can span multiple items or tables in DynamoDB.  Thread-safe.  
 * 
 * If you are using transactions on items in a table, you should perform write operations only using this transaction library. 
 * Failing to do so (performing raw writes to the could cause transactions to become stuck permanently, lost writes (either 
 * from transactions or from your writes), or other undefined behavior. 
 * 
 * These transactions are atomic and can provide read isolation.
 * <ul> 
 *   <li>Atomicity: The transaction guarantees that if you successfully commit your transaction, all of the requests in your transactions 
 *       will eventually be applied without interference from other transactions.  If your application dies while committing, other 
 *       transactions that attempt to lock any of the items in your transactions will finish your transaction before making progress in their own.
 *       It is recommended that you periodically scan the Transactions table for stuck transactions, or look for stale locks when you read items, 
 *       to ensure that transactions are eventually either re-driven or rolled back.</li>
 *   <li>Isolation: This library offers 3 forms of read isolation.  The strongest form involves acquiring read locks within the scope of a transaction.
 *       Once you commit the transaction, you know that those reads were performed in isolation.  While this is the strongest form, it is also the most
 *       expensive.  For other forms of read isolation, see {@link TransactionManager}.</li>
 * </ul>
 * 
 * Usage notes:
 * <ul>
 *   <li>When involving an item in a transaction that does not yet exist in the table, this library will insert the item without any other attributes 
 *       except for the primary key and some transaction metadata attributes.  If you perform reads on your table outside of the transaction library,
 *       you need to be prepared to deal with these "half written" items. These are identifiable by the presence of a "_TxT" attribute.  See 
 *       getItem in {@link TransactionManager} for read options that handle dealing with these "half written" items for you.</li>
 *   <li>You can't perform multiple write operations on the same item within the transaction</li>
 *   <li>If you read in a transaction after a write, the read will return the item as if the write has committed.</li>
 *   <li>Read locks may be upgraded to write locks, and you can read items if the have write locks.</li>
 *   <li>ReturnValues in write operations are supported.<li>
 *   <li>You are recommended to periodically scan your transactions table for stuck transactions so that they are eventually redriven or rolled back</li>
 * </ul>
 * 
 * Current caveats:
 * <ul>
 *   <li>The total amount of request data in a transaction may not exceed 64 KB.</li>
 *   <li>Conditions in write operations are not supported.</li>
 *   <li>This library cannot operate on items which are larger than 63 KB (TODO come up with exact value)</li>
 *   <li>Attributes beginning with "_Tx" are not allowed in your items involved in transactions.</li> 
 * </li> 
 */
public class Transaction {
    private static final Log LOG = LogFactory.getLog(Transaction.class);

    private static final int ITEM_LOCK_ACQUIRE_ATTEMPTS = 3;
    private static final int ITEM_COMMIT_ATTEMPTS = 2;
    private static final int TX_LOCK_ACQUIRE_ATTEMPTS = 2;
    private static final int TX_LOCK_CONTENTION_RESOLUTION_ATTEMPTS = 3;
    protected static final String BOOLEAN_TRUE_ATTR_VAL = "1";
    
    /* Attribute name constants */
    protected static final String TX_ATTR_PREFIX = "_Tx";
    public static final Set<String> SPECIAL_ATTR_NAMES;
    
    private final TransactionManager txManager;
    private TransactionItem txItem;
    private final String txId;
    private final TreeSet<Integer> fullyAppliedRequests = new TreeSet<Integer>();
    
    static {
        Set<String> names = new HashSet<String>(AttributeName.values().length);
        for(AttributeName name : AttributeName.values()) {
            names.add(name.toString());
        }
        SPECIAL_ATTR_NAMES = Collections.unmodifiableSet(names);
    }

    /**
     * default constructor to make cglib/spring able to proxy Transactions.
     */
    protected Transaction() {
        this.txManager = null;
        this.txItem = null;
        this.txId = null;
    }

    /**
     * Opens a new transaction inserts it into the database, or resumes an existing transaction.
     * 
     * @param txId
     * @param txManager
     * @param insert - whether or not this is a new transaction, or one being resumed.
     * @throws TransactionNotFoundException
     */
    protected Transaction(String txId, TransactionManager txManager, boolean insert) throws TransactionNotFoundException {
        this.txManager = txManager;
        this.txItem = new TransactionItem(txId, txManager, insert);        
        this.txId = txId;
    }
    
    /**
     * Resumes an existing transaction.  The caller must provide all of the attributes of the item.   
     * 
     * @param txItem
     * @param txManager
     * @throws TransactionNotFoundException
     */
    protected Transaction(Map<String, AttributeValue> txItem, TransactionManager txManager) throws TransactionNotFoundException {
        this.txManager = txManager;
        this.txItem = new TransactionItem(txItem, txManager);        
        this.txId = this.txItem.txId;
    }
    
    public String getId() {
        return txId;
    }
    
    /**
     * Adds a PutItem request to the transaction
     * 
     * @param request
     * @throws DuplicateRequestException if the item in the request is already involved in this transaction
     * @throws ItemNotLockedException when another transaction is confirmed to have the lock on the item in the request
     * @throws TransactionCompletedException when the transaction has already completed
     * @throws TransactionNotFoundException if the transaction does not exist
     * @throws TransactionException on unexpected errors or unresolvable OCC contention
     */
    public PutItemResult putItem(PutItemRequest request) 
        throws DuplicateRequestException, ItemNotLockedException, 
            TransactionCompletedException, TransactionNotFoundException, TransactionException {
        
        PutItem wrappedRequest = new PutItem();
        wrappedRequest.setRequest(request);
        Map<String, AttributeValue> item = driveRequest(wrappedRequest);
        stripSpecialAttributes(item);
        return new PutItemResult().withAttributes(item);
    }

    /**
     * Adds an UpdateItem request to the transaction
     * 
     * @param request
     * @throws DuplicateRequestException if the item in the request is already involved in this transaction
     * @throws ItemNotLockedException when another transaction is confirmed to have the lock on the item in the request
     * @throws TransactionCompletedException when the transaction has already completed
     * @throws TransactionNotFoundException if the transaction does not exist
     * @throws TransactionException on unexpected errors or unresolvable OCC contention
     */
    public UpdateItemResult updateItem(UpdateItemRequest request) 
        throws DuplicateRequestException, ItemNotLockedException, 
            TransactionCompletedException, TransactionNotFoundException, TransactionException {
        
        UpdateItem wrappedRequest = new UpdateItem();
        wrappedRequest.setRequest(request);
        Map<String, AttributeValue> item = driveRequest(wrappedRequest);
        stripSpecialAttributes(item);
        return new UpdateItemResult().withAttributes(item);
    }

    /**
     * Adds a DeleteItem request to the transaction
     * 
     * @param request
     * @throws DuplicateRequestException if the item in the request is already involved in this transaction
     * @throws ItemNotLockedException when another transaction is confirmed to have the lock on the item in the request
     * @throws TransactionCompletedException when the transaction has already completed
     * @throws TransactionNotFoundException if the transaction does not exist
     * @throws TransactionException on unexpected errors or unresolvable OCC contention
     */
    public DeleteItemResult deleteItem(DeleteItemRequest request) 
        throws DuplicateRequestException, ItemNotLockedException, 
            TransactionCompletedException, TransactionNotFoundException, TransactionException {
        
        DeleteItem wrappedRequest = new DeleteItem();
        wrappedRequest.setRequest(request);
        Map<String, AttributeValue> item = driveRequest(wrappedRequest);
        stripSpecialAttributes(item);
        return new DeleteItemResult().withAttributes(item);
    }
    
    /**
     * Locks an item for the duration of the transaction, unless it is already locked. Useful for isolated reads.  
     * Returns the copy of the item as it exists so far in the transaction (if reading after a write in the same transaction)
     * 
     * @param request
     * @throws DuplicateRequestException if the item in the request is already involved in this transaction
     * @throws ItemNotLockedException when another transaction is confirmed to have the lock on the item in the request
     * @throws TransactionCompletedException when the transaction has already completed
     * @throws TransactionNotFoundException if the transaction does not exist
     * @throws TransactionException on unexpected errors or unresolvable OCC contention
     */
    public GetItemResult getItem(GetItemRequest request)
        throws DuplicateRequestException, ItemNotLockedException, 
            TransactionCompletedException, TransactionNotFoundException, TransactionException {
        
        GetItem wrappedRequest = new GetItem();
        wrappedRequest.setRequest(request);
        Map<String, AttributeValue> item = driveRequest(wrappedRequest);
        stripSpecialAttributes(item);
        GetItemResult result = new GetItemResult().withItem(item);
        return result;
    }

    public static void stripSpecialAttributes(Map<String, AttributeValue> item) {
        if(item == null) {
            return;
        }
        for(String specialAttribute : SPECIAL_ATTR_NAMES) {
            item.remove(specialAttribute);
        }
    }
    
    public static boolean isLocked(Map<String, AttributeValue> item) {
        if(item == null) {
            return false;
        }
        if(item.containsKey(AttributeName.TXID.toString())) {
            return true;
        }
        return false;
    }
    
    public static boolean isApplied(Map<String, AttributeValue> item) {
        if(item == null) {
            return false;
        }
        if(item.containsKey(AttributeName.APPLIED.toString())) {
            return true;
        }
        return false;
    }
    
    public static boolean isTransient(Map<String, AttributeValue> item) {
        if(item == null) {
            return false;
        }
        if(item.containsKey(AttributeName.TRANSIENT.toString())) {
            return true;
        }
        return false;
    }
    
    public enum IsolationLevel {
        UNCOMMITTED,
        COMMITTED,
        READ_LOCK // what does it mean to read an item you wrote to in a transaction?
    }
    
    /**
     * Deletes the transaction.  
     * 
     * @return true if the transaction was deleted, false if it was not
     * @throws TransactionException if the transaction is not yet completed.
     */
    public boolean delete() throws TransactionException {
        return deleteIfAfter(null);
    }
    /**
     * Deletes the transaction, only if it has not been update since the specified duration.  A transaction's 
     * "last updated date" is updated when:
     *  - A request is added to the transaction
     *  - The transaction switches to COMMITTED or ROLLED_BACK
     *  - The transaction is marked as completed.  
     * 
     * @param deleteIfAfterMillis the duration to ensure has passed before attempting to delete the record
     * @return true if the transaction was deleted, false if it was not old enough to delete yet.
     * @throws TransactionException if the transaction is not yet completed.
     */
    public boolean delete(long deleteIfAfterMillis) throws TransactionException {
        return deleteIfAfter(deleteIfAfterMillis);
    }
    
    private synchronized boolean deleteIfAfter(Long deleteIfAfterMillis) throws TransactionException {
        if(! txItem.isCompleted()) {
            // Ensure we have an up to date tx record
            try {
                txItem = new TransactionItem(txId, txManager, false);
            } catch (TransactionNotFoundException e) {
                return true; // expected, transaction already deleted
            }
            if(! txItem.isCompleted()) {
                throw new TransactionException(txId, "You can only delete a transaction that is completed");
            }
        }
        try {
            if(deleteIfAfterMillis == null || (txItem.getLastUpdateTimeMillis() + deleteIfAfterMillis) < System.currentTimeMillis()) {
                txItem.delete();
                return true;
            }
        } catch (ConditionalCheckFailedException e) {
            // Can only happen if the tx isn't finalized or is already gone.
            try {
                txItem = new TransactionItem(txId, txManager, false);
                throw new TransactionException(txId, "Transaction was completed but could not be deleted. " + txItem);
            } catch (TransactionNotFoundException tnfe) {
                return true; // expected, transaction already deleted
            }
        }
        return false;
    }
    
    /**
     * Finishes a a transaction if it is already COMMITTED or PENDING but not yet COMPLETED
     * 
     * If it is PENDING and hasn't been active for rollbackAfterDurationMills, the transaction is rolled back.
     * 
     * If it is completed and hasn't been active for deleteAfterDurationMillis, the transaction is deleted.
     * 
     * @param rollbackAfterDurationMills
     * @param deleteAfterDurationMillis
     */
    public void sweep(long rollbackAfterDurationMills, long deleteAfterDurationMillis) {
        // If the item has been completed for the specified threshold, delete it.
        if(txItem.isCompleted()) {
            delete(deleteAfterDurationMillis);
        } else {
            // If the transaction has been PENDING for too long, roll it back.
            // If it's COMMITTED or PENDING, drive it to completion. 
            switch (txItem.getState()) {
                case PENDING:
                    if((txItem.getLastUpdateTimeMillis() + rollbackAfterDurationMills) < System.currentTimeMillis()) {
                        try {
                            rollback();
                        } catch (TransactionCompletedException e) {
                            // Transaction is already completed, ignore
                        }
                    }
                    break;
                case COMMITTED: // NOTE: falling through to ROLLED_BACK
                case ROLLED_BACK:
                    // This could call either commit or rollback - they'll both do the right thing if it's already committed
                    try {
                        rollback();
                    } catch (TransactionCompletedException e) {
                        // Transaction is already completed, ignore
                    }
                    break;
                default:
                    throw new TransactionAssertionException(txId, "Unexpected state in transaction: " + txItem.getState());
            }
        }
    }
   
    /**
     * Adds a request to the transaction
     * 
     * @param clientRequest
     * @throws DuplicateRequestException if the item in the request is already used in this transaction 
     * @throws ItemNotLockedException if we were unable to acquire the lock because of contention with other transactions 
     * @throws TransactionException if another unresolvable error occurs, including too much contention on this transaction record
     */
    protected synchronized Map<String, AttributeValue> driveRequest(Request clientRequest) throws DuplicateRequestException, ItemNotLockedException, TransactionException {
        /* 1. Validate the request (no conditions, required attributes, no conflicting attributes etc)
         * 2. Copy the request (we change things in it, so don't mutate the caller's request
         * 3. Acquire the lock, save image, apply via addRequest()
         *    a) If that fails, go to 3) again.
         */
        
        // basic validation
        clientRequest.validate(txId, txManager);
        
        // Don't mutate the caller's request.
        Request requestCopy = Request.deserialize(txId, Request.serialize(txId, clientRequest));
        
        ItemNotLockedException lastConflict = null;
        for(int i = 0; i < TX_LOCK_CONTENTION_RESOLUTION_ATTEMPTS; i++) {
            try {
                Map<String, AttributeValue> item = addRequest(requestCopy, (i != 0), TX_LOCK_ACQUIRE_ATTEMPTS);
                return item;
            } catch (ItemNotLockedException e) {
                // Roll back or complete the other transaction
                lastConflict = e;
                Transaction conflictingTransaction = null;
                try {
                    conflictingTransaction = new Transaction(e.getLockOwnerTxId(), txManager, false);
                    conflictingTransaction.rollback();
                } catch (TransactionNotFoundException tnfe) {
                    // ignore, and try again on the next iteration, previous lock should be gone now
                } catch (TransactionCompletedException completedException) {
                    // ignore, doesn't matter if it committed or rolled back
                }
            }
        }
        throw lastConflict;
    }

    /**
     * Commits the transaction
     * 
     * @throws TransactionRolledBackException - the transaction was rolled back by a concurrent overlapping transaction
     * @throws UnknownCompletedTransactionException - the transaction completed, but it is not known whether it committed or rolled back
     * TODO throw a specific exception for encountering too much contention 
     */
    public synchronized void commit() throws TransactionRolledBackException, UnknownCompletedTransactionException {
        // 1. Re-read transaction item
        //    a) If it doesn't exist, throw UnknownCompletedTransaction 
        
        // 2. Verify that we should continue
        //    a) If the transaction is closed, return or throw depending on COMPLETE or ROLLED_BACK.
        //    b) If the transaction is ROLLED_BACK, continue doRollback(), but at the end throw.
        //    c) If the transaction is COMMITTED, go to doCommit(), and at the end return.
        
        // 3. Save the transaction's version number to detect if additional requests are added
        
        // 4. Verify that we have all of the locks and their saved images
        
        // 5. Change the state to COMMITTED conditioning on: 
        //         - it isn't closed
        //         - the version number hasn't changed
        //         - it's still PENDING
        //      a) If that fails, go to 1).
        
        // 6. Return success
        
        for(int i = 0; i < ITEM_COMMIT_ATTEMPTS + 1; i++) {
            // Re-read state to ensure this isn't a resume that's going to come along and re-apply a completed transaction.
            // This doesn't prevent a transaction from being applied multiple times, but it prevents a sweeper from applying
            // a very old transaction.
            try {
                txItem = new TransactionItem(txId, txManager, false);
            } catch (TransactionNotFoundException tnfe) {
                throw new UnknownCompletedTransactionException(txId, "In transaction " + State.COMMITTED + " attempt, transaction either rolled back or committed");
            }
   
            if(txItem.isCompleted()) {
                if(State.COMMITTED.equals(txItem.getState())) {
                    return;
                } else if(State.ROLLED_BACK.equals(txItem.getState())) {
                    throw new TransactionRolledBackException(txId, "Transaction was rolled back");
                } else {
                    throw new TransactionAssertionException(txId, "Unexpected state for transaction: " + txItem.getState());
                }
            }
            
            if(State.COMMITTED.equals(txItem.getState())) {
                doCommit();
                return;
            }
            
            if(State.ROLLED_BACK.equals(txItem.getState())) {
                doRollback();
                throw new TransactionRolledBackException(txId, "Transaction was rolled back");
            }
            
            // Commit attempts is actually for the number of times we try to acquire all the locks
            if(! (i < ITEM_COMMIT_ATTEMPTS)) {
                throw new TransactionException(txId, "Unable to commit transaction after " + ITEM_COMMIT_ATTEMPTS + " attempts");
            }
            
            int version = txItem.getVersion();
            
            verifyLocks();
            
            try {
                txItem.finish(State.COMMITTED, version);
            } catch (ConditionalCheckFailedException e) {
                // Tx item version, changed out from under us, or was moved to committed, rolled back, deleted, etc by someone else.
                // Retry in loop
            }
        }
        
        throw new TransactionException(txId, "Unable to commit transaction after " + ITEM_COMMIT_ATTEMPTS + " attempts");
    }
    
    /**
     * Rolls back the transaction.  You can only roll back a transaction that is in the PENDING state (not yet committed).
     * <li>If you roll back a transaction in COMMITTED, this will continue committing the transaction if it isn't completed yet, 
     *      but you will get back a TransactionCommittedException. </li>
     * <li>If you roll back and already rolled back transaction, this will ensure the rollback completed, and return success</li>
     * <li>If the transaction no longer exists, you'll get back an UnknownCompletedTransactionException</li>   
     * 
     * @throws TransactionCommittedException - the transaction was committed by a concurrent overlapping transaction
     * @throws UnknownCompletedTransactionException - the transaction completed, but it is not known whether it was rolled back or committed
     */
    public synchronized void rollback() throws TransactionCompletedException, UnknownCompletedTransactionException {
        State state = null;
        boolean alreadyRereadTxItem = false;
        try {
            txItem.finish(State.ROLLED_BACK, txItem.getVersion());  
            state = State.ROLLED_BACK;
        } catch (ConditionalCheckFailedException e) {         
            try {
                // Re-read state to see its actual state, since it wasn't in PENDING
                txItem = new TransactionItem(txId, txManager, false);
                alreadyRereadTxItem = true;
                state = txItem.getState();
            } catch (TransactionNotFoundException tnfe) {
                throw new UnknownCompletedTransactionException(txId, "In transaction " + State.ROLLED_BACK + " attempt, transaction either rolled back or committed");
            }
        }
        
        if(State.COMMITTED.equals(state)) {
            if(! txItem.isCompleted()) {
                doCommit();
            }
            throw new TransactionCommittedException(txId, "Transaction was committed");
        } else if(State.ROLLED_BACK.equals(state)) {
            if(! txItem.isCompleted()) {
                doRollback();
            }
            return;
        } else if (State.PENDING.equals(state)) {
            if (! alreadyRereadTxItem) {
                // The item was modified in the meantime (another request was added to it)
                // so make sure we re-read it, and then try the rollback again
                txItem = new TransactionItem(txId, txManager, false);
            }
            rollback();
            return;
        }
        throw new TransactionAssertionException(txId, "Unexpected state in rollback(): " + state);
    }
    
    /**
     * Verifies that we actually hold all of the locks for the requests in the transaction, and that we have saved the 
     * previous item images of every item involved in the requests (except for request types that we don't save images for).
     * 
     * The caller needs to wrap this with OCC on the tx version (request count) if it's going to commit based on this decision.
     * 
     * This is optimized to consider the "version" numbers of the items that this Transaction object has fully applied so far
     * to optimize the normal case that doesn't have failures.
     */
    protected void verifyLocks() {
        for(Request request : txItem.getRequests()) {
            // Optimization: If our transaction object (this) has first-hand fully applied a request, no need to do it again.
            if(! fullyAppliedRequests.contains(request.getRid())) {
                addRequest(request, true, ITEM_LOCK_ACQUIRE_ATTEMPTS);    
            }
        }
    }
    
    /**
     * Deletes the transaction item from the database.
     * 
     * Does not throw if the item is gone, even if the conditional check to delete the item fails, and this method doesn't know what state
     * it was in when deleted.  The caller is responsible for guaranteeing that it was actually in "currentState" immediately before calling
     * this method.  
     */
    protected void complete(final State expectedCurrentState) {
        try {
            txItem.complete(expectedCurrentState);
        } catch (ConditionalCheckFailedException e) {
            // Re-read state to ensure it was already completed
            try {
                txItem = new TransactionItem(txId, txManager, false);
                if(! txItem.isCompleted()) {
                    throw new TransactionAssertionException(txId, "Expected the transaction to be completed (no item), but there was one.");
                }
            } catch (TransactionNotFoundException tnfe) {
                // expected - transaction record no longer exists
            }
        }
    }

    /**
     * Deletes the old item images and unlocks each item, deleting the item themselves if they inserted only to lock the item.  
     * 
     * This is to be used post-commit only.
     */
    protected void doCommit() {
        // Defensively re-check the state to ensure it is COMMITTED
        txAssert(txItem != null && State.COMMITTED.equals(txItem.getState()), txId, "doCommit() requires a non-null txItem with a state of " + State.COMMITTED, "state", txItem.getState(), "txItem", txItem);
        
        // Note: Order is functionally unimportant, but we unlock all items first to try to reduce the need 
        // for other readers to read this transaction's information since it has already committed.
        for(Request request : txItem.getRequests()) {
            //Unlock the item, deleting it if it was inserted only to lock the item, or if it was a delete request
            unlockItemAfterCommit(request);
        }
            
        // Clean up the old item images
        for(Request request : txItem.getRequests()) {
            txItem.deleteItemImage(request.getRid());
        }
        
        complete(State.COMMITTED);
    }
    
    /**
     * Releases the lock for the item.  If the item was inserted only to acquire the lock (if the item didn't exist before 
     * for a DeleteItem or LockItem), it will be deleted now.
     * 
     * Otherwise, all of the attributes uses for the transaction (tx id, transient flag, applied flag) will be removed.
     * 
     * Conditions on our transaction id owning the item
     * 
     * To be used once the transaction has committed only.
     * @param request
     */
    protected void unlockItemAfterCommit(Request request) {
        try {
            Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
            expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(txId)));
            
            if(request instanceof PutItem || request instanceof UpdateItem) {
                Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
                updates.put(AttributeName.TXID.toString(), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                updates.put(AttributeName.TRANSIENT.toString(), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                updates.put(AttributeName.APPLIED.toString(), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                updates.put(AttributeName.DATE.toString(), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                
                UpdateItemRequest update = new UpdateItemRequest()
                    .withTableName(request.getTableName())
                    .withKey(request.getKey(txManager))
                    .withAttributeUpdates(updates)
                    .withExpected(expected);
                txManager.getClient().updateItem(update);
            } else if(request instanceof DeleteItem) {
                DeleteItemRequest delete = new DeleteItemRequest()
                    .withTableName(request.getTableName())
                    .withKey(request.getKey(txManager))
                    .withExpected(expected);
                txManager.getClient().deleteItem(delete);
            } else if(request instanceof GetItem) {
                releaseReadLock(request.getTableName(), request.getKey(txManager));
            } else {
                throw new TransactionAssertionException(txId, "Unknown request type: " + request.getClass());
            }
        } catch (ConditionalCheckFailedException e) {
            // ignore, unlock already happened
            // TODO if we really want to be paranoid we could condition on applied = 1, and then here
            //      we would have to read the item again and make sure that applied was 1 if we owned the lock (and assert otherwise) 
        }
    }
    
    /**
     * Rolls back the transaction, only if the transaction is in the ROLLED_BACK state.
     * 
     * This handles using the AttributeName.TRANSIENT to ensure that if an item was "phantom" (inserted during the transaction when acquiring the lock),
     * it gets deleted on rollback.
     */
    protected void doRollback() {
        txAssert(State.ROLLED_BACK.equals(txItem.getState()), txId, "Transaction state is not " + State.ROLLED_BACK, "state", txItem.getState(), "txItem", txItem);
        
        for(Request request : txItem.getRequests()) {
            // Unlike unlockItems(), the order is important here.
            
            // 1. Apply the old item image over the one the request modified 
            rollbackItemAndReleaseLock(request);
            
            // 2. Delete the old item image, we don't need it anymore
            txItem.deleteItemImage(request.getRid());
        }
        
        complete(State.ROLLED_BACK);
    }
    
    /**
     * Rolls back the apply of the request by reading the previous item image and overwriting the item with the old image.
     * If there was no old item image, determines whether the item was transient (and there shouldn't be an item image), 
     * or if   
     * 
     * In the case of lock requests, the lock is simply removed.
     * 
     * In either case, if the item did not exist before the lock was acquired, it is deleted.
     * 
     * @param request
     */
    protected void rollbackItemAndReleaseLock(Request request) {
        rollbackItemAndReleaseLock(request.getTableName(), request.getKey(txManager), request instanceof GetItem, request.getRid());
    }
        
    protected void rollbackItemAndReleaseLock(String tableName, Map<String, AttributeValue> key, Boolean isGet, Integer rid) {
        // TODO there seems to be a race that leads to orphaned old item images (but is still correct in terms of the transaction)
        // A previous master could have stalled after writing the tx record, fall asleep, and then finally insert the old item image 
        // after this delete attempt goes through, and then the sleepy master crashes. There's no great way around this, 
        // so a sweeper needs to deal with it.
        
        // Possible outcomes:
        // 1) We know for sure from just the request (getItem) that we never back up the item. Release the lock (and delete if transient)
        // 2) We found a backup.  Apply the backup.
        // 3) We didn't find a backup. Try deleting the item with expected: 1) Transient, 2) Locked by us, return success
        // 4) Read the item. If we don't have the lock anymore, meaning it was already rolled back.  Return.
        // 5) We failed to take the backup, but should have.  
        //   a) If we've applied, assert.  
        //   b) Otherwise release the lock (okay to delete if transient to re-use logic)
       
        // 1. Read locks don't have a saved item image, so just unlock them and return
        if(isGet != null && isGet) {
            releaseReadLock(tableName, key);
            return;
        }

        // Read the old item image, if the rid is known.  Otherwise we treat it as if we don't have an item image.
        Map<String, AttributeValue> itemImage = null;
        if(rid != null) {
            itemImage = txItem.loadItemImage(rid);
        }
        
        if(itemImage != null) {
            // 2. Found a backup.  Replace the current item with the pre-changes version of the item, at the same time removing the lock attributes
            txAssert(itemImage.remove(AttributeName.TRANSIENT.toString()) == null, txId, "Didn't expect to have saved an item image for a transient item", "itemImage", itemImage);
            
            itemImage.remove(AttributeName.TXID.toString());
            itemImage.remove(AttributeName.DATE.toString());
            
            txAssert(! itemImage.containsKey(AttributeName.APPLIED.toString()), txId, "Old item image should not have contained the attribute " + AttributeName.APPLIED.toString(), "itemImage", itemImage);
            
            try {
                Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
                expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(txId)));
                
                PutItemRequest put = new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(itemImage)
                    .withExpected(expected);
                txManager.getClient().putItem(put);
            } catch (ConditionalCheckFailedException e) {
                // Only conditioning on "locked by us", so if that fails, it means it already happened (and may have advanced forward)
            }
        } else {
            // 3) We didn't find a backup. Try deleting the item with expected: 1) Transient, 2) Locked by us, return success
            try {
                Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
                expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(txId)));
                expected.put(AttributeName.TRANSIENT.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(BOOLEAN_TRUE_ATTR_VAL)));
                
                DeleteItemRequest delete = new DeleteItemRequest()
                    .withTableName(tableName)
                    .withKey(key)
                    .withExpected(expected);
                txManager.getClient().deleteItem(delete);
                return;
            } catch (ConditionalCheckFailedException e) {
                // This means it already happened (and may have advanced forward)
                // Technically there could be a bug if it is locked by us but not marked as transient.
            }
            // 4) Read the item. If we don't have the lock anymore, meaning it was already rolled back.  Return.
            // 5) We failed to take the backup, but should have.  
            //   a) If we've applied, assert.  
            //   b) Otherwise release the lock (okay to delete if transient to re-use logic)
            
            // 4) Read the item. If we don't have the lock anymore, meaning it was already rolled back.  Return.
            Map<String, AttributeValue> item = getItem(tableName, key);
            
            if(item == null || ! txId.equals(getOwner(item))) {
                // 3a) We don't have the lock anymore.  Return.
                return;
            }
            
            // 5) We failed to take the backup, but should have.  
            //   a) If we've applied, assert.
            txAssert(! item.containsKey(AttributeName.APPLIED.toString()), txId, "Applied change to item but didn't save a backup", "table", tableName, "key", key, "item" + item);
            
            //   b) Otherwise release the lock (okay to delete if transient to re-use logic)
            releaseReadLock(tableName, key);
        }
    }
    
    /**
     * Unlocks an item without applying the previous item image on top of it.  This will delete the item if it 
     * was marked as phantom.  
     * 
     * This is ONLY valid for releasing a read lock (either during rollback or post-commit) 
     *  OR releasing a lock where the change wasn't applied yet.
     * 
     * @param tableName
     * @param key
     */
    protected void releaseReadLock(String tableName, Map<String, AttributeValue> key) {
        releaseReadLock(txId, txManager, tableName, key);
    }
    
    protected static void releaseReadLock(String txId, TransactionManager txManager, String tableName, Map<String, AttributeValue> key) {
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
        expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(txId)));
        expected.put(AttributeName.TRANSIENT.toString(), new ExpectedAttributeValue().withExists(false));
        expected.put(AttributeName.APPLIED.toString(), new ExpectedAttributeValue().withExists(false));
        
        try {
            Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>(1);
            updates.put(AttributeName.TXID.toString(), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
            updates.put(AttributeName.DATE.toString(), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
            
            UpdateItemRequest update = new UpdateItemRequest()
                .withTableName(tableName)
                .withAttributeUpdates(updates)
                .withKey(key)
                .withExpected(expected);
            txManager.getClient().updateItem(update);
        } catch (ConditionalCheckFailedException e) {
            try {
                expected.put(AttributeName.TRANSIENT.toString(), new ExpectedAttributeValue().withValue(new AttributeValue().withS(BOOLEAN_TRUE_ATTR_VAL)));
                
                DeleteItemRequest delete = new DeleteItemRequest()
                    .withTableName(tableName)
                    .withKey(key)
                    .withExpected(expected);
                txManager.getClient().deleteItem(delete);    
            } catch (ConditionalCheckFailedException e1) {
                // Ignore, means it was definitely rolled back
                // Re-read to ensure that it wasn't applied
                Map<String, AttributeValue> item = getItem(txManager, tableName, key);
                txAssert(! (item != null && txId.equals(getOwner(item)) && item.containsKey(AttributeName.APPLIED.toString())), 
                    "Item should not have been applied.  Unable to release lock", "item", item);
            }
        }
    }
    
    /**
     * Unlocks an item and leaves it in an unknown state, as long as there is no associated transaction record
     * 
     * @param txManager
     * @param tableName
     * @param item
     */
    protected static void unlockItemUnsafe(TransactionManager txManager, String tableName, Map<String, AttributeValue> item, String txId) {
        
        // 1) Ensure the transaction does not exist 
        try {
            Transaction tx = new Transaction(txId, txManager, false);
            throw new TransactionException(txId, "The transaction item should not have existed, but it did.  You can only unsafely unlock an item without a tx record. txItem: " + tx.txItem);
        } catch (TransactionNotFoundException e) {
            // Expected to not exist
        }


        // 2) Remove all transaction attributes and condition on txId equality
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
        expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(txId)));
        
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>(1);
        for(String attrName : SPECIAL_ATTR_NAMES) {
            updates.put(attrName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
        }
        
        Map<String, AttributeValue> key = Request.getKeyFromItem(tableName, item, txManager);
        
        UpdateItemRequest update = new UpdateItemRequest()
            .withTableName(tableName)
            .withAttributeUpdates(updates)
            .withKey(key)
            .withExpected(expected);
        
        // Delete the item, and ignore conditional write failures
        try {
            txManager.getClient().updateItem(update);
        } catch (ConditionalCheckFailedException e) { 
            // already unlocked
        }
    }
    
    /**
     * Adds a request to the transaction, preserving order of requests via the version field in the tx record  
     *  
     * @param callerRequest
     * @param isRedrive - true if the request was already saved to the tx item, and this is redriving the attempt to write the tx to the item (fighting for a lock with other transactions) 
     * @param numAttempts
     * @throws DuplicateRequestException if the item in the request is already involved in this transaction
     * @throws ItemNotLockedException when another transaction is confirmed to have the lock on the item in the request
     * @throws TransactionCompletedException when the transaction has already completed
     * @throws TransactionNotFoundException if the transaction does not exist
     * @throws TransactionException on unexpected errors or unresolvable OCC contention
     * @return the applied item image, or null if the apply was a delete.
     */
    protected Map<String, AttributeValue> addRequest(Request callerRequest, boolean isRedrive, int numAttempts) throws DuplicateRequestException, 
        ItemNotLockedException, TransactionCompletedException, TransactionNotFoundException, TransactionException {
        
        // 1. Write the full caller request to the transaction item, but not if it's being re-driven.
        //    (In order to re-drive, the request must already be in the transaction item) 
        if(! isRedrive) {
            boolean success = false;
            for(int i = 0; i < numAttempts; i++) {
                // 1a. Verify the locks up to ensure that if we are adding a "read" request for an item that has been written to in this transaction,
                //     that we return the write.
                verifyLocks(); 
                try {
                    txItem.addRequest(callerRequest);
                    success = true;
                    break;
                } catch (ConditionalCheckFailedException e) {
                    // The transaction is either not in PENDING anymore, or the version number incremented from another thread/process
                    // registering a transaction (or we started cold on an existing transaction).
                    
                    txItem = new TransactionItem(txId, txManager, false);
                    
                    if(State.COMMITTED.equals(txItem.getState())) {
                        throw new TransactionCommittedException(txId, "Attempted to add a request to a transaction that was not in state " + State.PENDING + ", state is " + txItem.getState());
                    } else if(State.ROLLED_BACK.equals(txItem.getState())) {
                        throw new TransactionRolledBackException(txId, "Attempted to add a request to a transaction that was not in state " + State.PENDING + ", state is " + txItem.getState());
                    } else if(! State.PENDING.equals(txItem.getState())) {
                        throw new UnknownCompletedTransactionException(txId, "Attempted to add a request to a transaction that was not in state " + State.PENDING + ", state is " + txItem.getState());    
                    }
                }
            }
            
            if(! success) {
                throw new TransactionException(txId, "Unable to add request to transaction - too much contention for the tx record");
            }
        } else {
            txAssert(State.PENDING.equals(txItem.getState()), txId, "Attempted to add a request to a transaction that was not in state " + State.PENDING, "state", txItem.getState());
        }
        
        // 2. Write txId to item
        Map<String, AttributeValue> item = lockItem(callerRequest, true, ITEM_LOCK_ACQUIRE_ATTEMPTS);
        
        //    As long as this wasn't a duplicate read request,
        // 3. Save the item image to a new item in case we need to roll back, unless:
        //    - it's a lock request,
        //    - we've already saved the item image
        //    - the item is transient (inserted for acquiring the lock)
        saveItemImage(callerRequest, item);
        
        // 3a. Re-read the transaction item to make sure it hasn't been rolled back or completed.
        //     Can be optimized if we know the transaction is already completed(
        try {
            txItem = new TransactionItem(txId, txManager, false);
        } catch (TransactionNotFoundException e) {
            releaseReadLock(callerRequest.getTableName(), callerRequest.getKey(txManager));
            throw e;
        }
        switch (txItem.getState()) {
            case COMMITTED: 
                doCommit();
                throw new TransactionCommittedException(txId, "The transaction already committed");
            case ROLLED_BACK:
                doRollback();
                throw new TransactionRolledBackException(txId, "The transaction already rolled back");
            case PENDING:
                break;
            default:
                throw new TransactionException(txId, "Unexpected state " + txItem.getState());
        }
        
        // 4. Apply change to item, keeping lock on the item, returning the attributes according to RETURN_VALUE
        //    If we are a read request, and there is an applied delete request for the same item in the tx, return null.
        Map<String, AttributeValue> returnItem = applyAndKeepLock(callerRequest, item);
        
        // 5. Optimization: Keep track of the requests that this transaction object has fully applied
        if(callerRequest.getRid() != null) {
            fullyAppliedRequests.add(callerRequest.getRid());
        }
   
        return returnItem;
    }
    
    protected void saveItemImage(Request callerRequest, Map<String, AttributeValue> item) {
        if(isRequestSaveable(callerRequest, item) && ! item.containsKey(AttributeName.APPLIED.toString()) ) {
            txItem.saveItemImage(item, callerRequest.getRid());
        }
    }
    
    protected boolean isRequestSaveable(Request callerRequest, Map<String, AttributeValue> item) {
        if(! (callerRequest instanceof GetItem) && ! item.containsKey(AttributeName.TRANSIENT.toString())) {
            return true;
        }
        return false;
    }
    
    
    /**
     * Attempts to lock an item.  If the conditional write fails, we read the item to see if we already hold the lock.
     * If that read reveals no lock owner, then we attempt again to acquire the lock, for a total of "attempts" times.  
     * 
     * @param callerRequest
     * @param attempts
     * @return the locked item image
     * @throws ItemNotLockedException when the item is locked by another transaction 
     * @throws TransactionException when we ran out of attempts to write the item, but it did not appear to be owned
     */
    protected Map<String, AttributeValue> lockItem(Request callerRequest, boolean expectExists, int attempts) throws ItemNotLockedException, TransactionException {
        Map<String, AttributeValue> key = callerRequest.getKey(txManager);
        
        if(attempts <= 0) {
            throw new TransactionException(txId, "Unable to acquire item lock for item " + key); // This won't trigger a rollback, it's really just a case of contention and needs more redriving
        }
        
        // Create Expected and Updates maps.  
        //   - If we expect the item TO exist, we only update the lock
        //   - If we expect the item NOT to exist, we update both the transient attribute and the lock.
        // In both cases we expect the txid not to be set
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put(AttributeName.TXID.toString(), new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue(txId)));
        updates.put(AttributeName.DATE.toString(), new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(txManager.getCurrentTimeAttribute()));
        
        Map<String, ExpectedAttributeValue> expected;
        if(expectExists) {
            expected = callerRequest.getExpectExists(txManager);
            expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withExists(false));
        } else {
            expected = new HashMap<String, ExpectedAttributeValue>(1);
            updates.put(AttributeName.TRANSIENT.toString(), new AttributeValueUpdate()
                .withAction(AttributeAction.PUT)
                .withValue(new AttributeValue().withS(BOOLEAN_TRUE_ATTR_VAL)));
        }
        expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withExists(false));
        
        // Do a conditional update on NO transaction id, and that the item DOES exist
        UpdateItemRequest updateRequest = new UpdateItemRequest()
            .withTableName(callerRequest.getTableName())
            .withExpected(expected)
            .withKey(key)
            .withReturnValues(ReturnValue.ALL_NEW)
            .withAttributeUpdates(updates);
        
        String owner = null;
        boolean nextExpectExists = false;
        Map<String, AttributeValue> item = null;
        try {
            item = txManager.getClient().updateItem(updateRequest).getAttributes();
            owner = getOwner(item);
        } catch (ConditionalCheckFailedException e) {
            // If the check failed, it means there is either:
            //   1) a different transaction currently locking the item
            //   2) this transaction already is attempting to lock the item.  
            //   3) the item does not exist
            // Get the item and see which is the case
            item = getItem(callerRequest.getTableName(), key);
            if(item == null) {
                nextExpectExists = false;
            } else {
                nextExpectExists = true;
                owner = getOwner(item);
            }
        }
        
        // Try the write again if the item is unowned (but not if it is owned)
        if(owner != null) {
            if(txId.equals(owner)) {
                return item;
            }
            // For now, always roll back / complete the other transaction in the case of a conflict.
            if(attempts > 1) {
                try {
                    Transaction otherTransaction = txManager.resumeTransaction(owner);
                    otherTransaction.rollback();
                } catch (TransactionCompletedException e) {
                    // no-op
                } catch (TransactionNotFoundException e) {
                    releaseReadLock(owner, txManager, callerRequest.getTableName(), key);
                }
            } else {
                throw new ItemNotLockedException(txId, owner, callerRequest.getTableName(), key);
            }
        }
        return lockItem(callerRequest, nextExpectExists, attempts - 1);
    }
    

    /**
     * Writes the request to the user table and keeps the lock, as long as we still have the lock.
     * Ensures that the write happens (at most) once, because the write atomically marks the item as applied.
     * 
     * This is a no-op for DeleteItem or LockItem requests, since for delete the item isn't removed until after
     * the transaction commits, and lock doesn't mutate the item.
     * 
     * Note that this method mutates the item and the request.
     * 
     * @param request
     * @param lockedItem
     * @return the copy of the item, as requested in ReturnValues of the request (or the new item in the case of a read), or null if this is a redrive 
     */
    protected Map<String, AttributeValue> applyAndKeepLock(Request request, Map<String, AttributeValue> lockedItem) {
        Map<String, AttributeValue> returnItem = null;

        // 1. Remember what return values the caller wanted.
        String returnValues = request.getReturnValues(); // save the returnValues because we will mutate it
        if(returnValues == null) {
            returnValues = "NONE";
        }
        
        // 3. No-op if the locked item shows it was already applied.
        if(! lockedItem.containsKey(AttributeName.APPLIED.toString())) {
            try {
                Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
                expected.put(AttributeName.TXID.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(txId)));
                expected.put(AttributeName.APPLIED.toString(), new ExpectedAttributeValue().withExists(false));
                
                // TODO assert if the caller request contains any of our internally defined fields?
                //      but we aren't copying the request object, so our retries might trigger the assertion.
                //      at least could assert that they have the values that we want.
                if(request instanceof PutItem) {
                    PutItemRequest put = ((PutItem)request).getRequest();
                    // Add the lock id and "is transient" flags to the put request (put replaces) 
                    put.getItem().put(AttributeName.TXID.toString(), new AttributeValue(txId));
                    put.getItem().put(AttributeName.APPLIED.toString(), new AttributeValue(BOOLEAN_TRUE_ATTR_VAL));
                    if(lockedItem.containsKey(AttributeName.TRANSIENT.toString())) {
                        put.getItem().put(AttributeName.TRANSIENT.toString(), lockedItem.get(AttributeName.TRANSIENT.toString()));    
                    }
                    put.getItem().put(AttributeName.DATE.toString(), lockedItem.get(AttributeName.DATE.toString()));
                    put.setExpected(expected);
                    put.setReturnValues(returnValues);
                    returnItem = txManager.getClient().putItem(put).getAttributes();
                } else if(request instanceof UpdateItem) {
                    UpdateItemRequest update = ((UpdateItem)request).getRequest();
                    update.setExpected(expected);
                    update.setReturnValues(returnValues);
                    
                    if(update.getAttributeUpdates() != null) {
                        // Defensively delete the attributes in the request that could interfere with the transaction
                        update.getAttributeUpdates().remove(AttributeName.TXID.toString());
                        update.getAttributeUpdates().remove(AttributeName.TRANSIENT.toString());
                        update.getAttributeUpdates().remove(AttributeName.DATE.toString());
                    } else {
                        update.setAttributeUpdates(new HashMap<String, AttributeValueUpdate>(1));
                    }
                    
                    update.getAttributeUpdates().put(AttributeName.APPLIED.toString(), new AttributeValueUpdate()
                        .withAction(AttributeAction.PUT)
                        .withValue(new AttributeValue(BOOLEAN_TRUE_ATTR_VAL)));
                    
                    returnItem = txManager.getClient().updateItem(update).getAttributes();
                } else if(request instanceof DeleteItem) {
                    // no-op - delete doesn't change the item until unlock post-commit
                } else if(request instanceof GetItem) {
                    // no-op
                } else {
                    throw new TransactionAssertionException(txId, "Request may not be null");
                }
            } catch (ConditionalCheckFailedException e) {
                // ignore - apply already happened
            }
        }

        // If it is a redrive, don't return an item.
        // TODO propagate a flag for whether this is a caller request or if it's being redriven by another transaction manager picking it up.
        //      In that case it doesn't matter what we do here.
        //      Also change the returnValues in the write requests based on this.
        if("ALL_OLD".equals(returnValues) && isTransient(lockedItem)) {
            return null;
        } else if(request instanceof GetItem) {
            GetItemRequest getRequest = ((GetItem)request).getRequest();
            Request lockingRequest = txItem.getRequestForKey(request.getTableName(), request.getKey(txManager));
            if(lockingRequest instanceof DeleteItem) {
                return null; // If the item we're getting is deleted in this transaction
            } else if(lockingRequest instanceof GetItem && isTransient(lockedItem)) {
                return null; // If the item has only a read lock and is transient
            } else if(getRequest.getAttributesToGet() != null) {
                // Remove attributes that weren't asked for in the request
                Set<String> attributesToGet = new HashSet<String>(getRequest.getAttributesToGet());
                Iterator<Map.Entry<String, AttributeValue>> it = lockedItem.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, AttributeValue> attr = it.next();
                    if(! attributesToGet.contains(attr.getKey())) {
                        it.remove(); // TODO does this need to keep the tx attributes?
                    }
                }
            }
            return lockedItem;
        } else if(request instanceof DeleteItem) {
            if("ALL_OLD".equals(returnValues)) {
                return lockedItem; // Deletes are left alone in apply, so return the locked item
            } 
            return null; // In the case of NONE or ALL_NEW, it doesn't matter - item is (being) deleted.
        } else if("ALL_OLD".equals(returnValues)) {
            if(returnItem != null) {
                return returnItem; // If the apply write succeeded, we have the ALL_OLD from the request
            }
            returnItem = txItem.loadItemImage(request.getRid());
            if(returnItem == null) {
                throw new UnknownCompletedTransactionException(txId, "Transaction must have completed since the old copy of the image is missing");
            }
            return returnItem;
        } else if("ALL_NEW".equals(returnValues)) {
            if(returnItem != null) {
                return returnItem; // If the apply write succeeded, we have the ALL_NEW from the request
            }
            returnItem = getItem(request.getTableName(), request.getKey(txManager));
            if(returnItem == null) {
                throw new UnknownCompletedTransactionException(txId, "Transaction must have completed since the item no longer exists");
            }
            String owner = getOwner(returnItem);
            if(! txId.equals(owner)) {
                throw new ItemNotLockedException(txId, owner, request.getTableName(), returnItem);
            }
            return returnItem;
        } else if("NONE".equals(returnValues)) {
            return null;
        } else {
            throw new TransactionAssertionException(txId, "Unsupported return values: " + returnValues);
        }
    }
    
    /**
     * Returns a copy of the requested item all attributes retrieved.  Performs a consistent read.
     *  
     * @param tableName
     * @param key
     * @return the item map, with all attributes fetched
     */
    protected Map<String, AttributeValue> getItem(String tableName, Map<String, AttributeValue> key) {
        return getItem(txManager, tableName, key);
    }
    
    protected static Map<String, AttributeValue> getItem(TransactionManager txManager, String tableName, Map<String, AttributeValue> key) {
        GetItemRequest getRequest = new GetItemRequest()
            .withTableName(tableName)
            .withConsistentRead(true)
            .withKey(key);
        GetItemResult getResult = txManager.getClient().getItem(getRequest);
        return getResult.getItem();
    }

    /**
     * Determines the current lock holder for the given item
     * 
     * @param item must not be null
     * @return the owning transaction id, or null if the item isn't locked
     */
    protected static String getOwner(Map<String, AttributeValue> item) {
        if(item == null) {
            throw new IllegalArgumentException();
        }
        AttributeValue itemTxId = item.get(AttributeName.TXID.toString()); 
        if(itemTxId != null && itemTxId.getS() != null) {
            return itemTxId.getS();
        }
        return null;
    }
    
    /**
     * For unit tests
     * @return the current transaction item
     */
    protected TransactionItem getTxItem() {
        return txItem;
    }
    
    public enum AttributeName {
        
        TXID(TX_ATTR_PREFIX + "Id"), 
        TRANSIENT(TX_ATTR_PREFIX + "T"),
        DATE(TX_ATTR_PREFIX + "D"),
        APPLIED(TX_ATTR_PREFIX + "A"),
        REQUESTS(TX_ATTR_PREFIX + "R"),
        STATE(TX_ATTR_PREFIX + "S"),
        VERSION(TX_ATTR_PREFIX + "V"),
        FINALIZED(TX_ATTR_PREFIX + "F"),
        IMAGE_ID(TX_ATTR_PREFIX + "I");
        
        private AttributeName(String value) {
            this.value = value;
        }
        
        private final String value;
        
        public String toString() {
            return value;
        }
    }

    /**
     * Delete an item using the mapper.
     *
     * @param item
     *            An item object with key attributes populated.
     */
    public <T> void delete(final T item) {
        doWithMapper(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                txManager.getClientMapper().delete(item);
                return null;
            }
        });
    }

    /**
     * Load an item using the mapper.
     *
     * @param item
     *            An item object with key attributes populated.
     * @return An instance of the item class with all attributes populated from
     *         the table, or null if the item does not exist as of the start of
     *         this transaction.
     */
    public <T> T load(final T item) {
        return doWithMapper(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return txManager.getClientMapper().load(item);
            }
        });
    }

    /**
     * Save an item using the mapper.
     *
     * @param item
     *            An item object with key attributes populated.
     */
    public <T> void save(final T item) {
        doWithMapper(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                txManager.getClientMapper().save(item);
                return null;
            }
        });
    }

    private <T> T doWithMapper(Callable<T> callable) {
        try {
            txManager.getFacadeProxy().setBackend(new TransactionDynamoDBFacade(this, txManager));
            return callable.call();
        } catch (RuntimeException e) {
            // have to do this here in order to avoid having to declare a checked exception type
            throw e;
        } catch (Exception e) {
            // none of the callers of this method need to throw a checked exception
            throw new RuntimeException(e);
        } finally {
            txManager.getFacadeProxy().setBackend(null);
        }
    }

}
