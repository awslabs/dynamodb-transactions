/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static com.amazonaws.services.dynamodbv2.transactions.Transaction.AttributeName;
import static com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException.txAssert;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.transactions.Request.GetItem;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.DuplicateRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.InvalidRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionNotFoundException;
import com.amazonaws.services.dynamodbv2.util.ImmutableKey;

/**
 * Contains an image of the transaction item in DynamoDB, and methods to change that item.
 * 
 * If any of those attempts to change the transaction fail, the item needs to be thrown away, re-fetched,
 * and the change applied via the new item.
 */
class TransactionItem {
    
    /* Transaction states */
    private static final String STATE_PENDING = "P";
    private static final String STATE_COMMITTED = "C";
    private static final String STATE_ROLLED_BACK = "R";

    protected final String txId;
    private final TransactionManager txManager;
    private Map<String, AttributeValue> txItem;
    private int version; 
    private final Map<String, AttributeValue> txKey;
    private final Map<String, HashMap<ImmutableKey, Request>> requestsMap = new HashMap<String, HashMap<ImmutableKey, Request>>();
    
    /*
     * Constructors and initializers
     */
    
    /**
     * Inserts or retrieves a transaction record. 
     * 
     * @param txId the id of the transaction to insert or retrieve
     * @param txManager
     * @param insert whether to insert the transaction (it's a new transaction) or to load an existing one
     * @throws TransactionNotFoundException If it is being retrieved and it is not found
     */
    public TransactionItem(String txId, TransactionManager txManager, boolean insert) throws TransactionNotFoundException {
        this(txId, txManager, insert, null);
    }
    
    public TransactionItem(Map<String, AttributeValue> txItem, TransactionManager txManager) throws TransactionNotFoundException {
        this(null, txManager, false, txItem);
    }
    
    /**
     * Either inserts a new transaction, reads it from the database, or initializes from a previously read transaction item.
     * 
     * @param txId
     * @param txManager
     * @param insert
     * @param txItem A previously read transaction item (must include all of the attributes from the item). May not be specified with txId.
     * @throws TransactionNotFoundException
     */
    protected TransactionItem(String txId, TransactionManager txManager, boolean insert, Map<String, AttributeValue> txItem) throws TransactionNotFoundException {
        this.txManager = txManager;
   
        // Initialize txId, txKey, and txItem
        if(txId != null) {
            // Validate mutual exclusivity of inputs
            if(txItem != null) {
                throw new TransactionException(txId, "When providing txId, txItem must be null");
            }
            this.txId = txId;
            Map<String, AttributeValue> txKeyMap = new HashMap<String, AttributeValue>(1);
            txKeyMap.put(AttributeName.TXID.toString(), new AttributeValue(txId));
            this.txKey = Collections.unmodifiableMap(txKeyMap);
            if(insert) {
                this.txItem = insert();
            } else {
                this.txItem = get();
                if(this.txItem == null) {
                    throw new TransactionNotFoundException(this.txId);
                }
            }
        } else if(txItem != null) {
            // Validate mutual exclusivity of inputs
            if(insert) {
                throw new TransactionException(txId, "When providing a txItem, insert must be false");
            }
            this.txItem = txItem;
            if(! isTransactionItem(txItem)) {
                throw new TransactionException(txId, "txItem is not a transaction item");
            }
            this.txId = txItem.get(AttributeName.TXID.toString()).getS();
            Map<String, AttributeValue> txKeyMap = new HashMap<String, AttributeValue>(1);
            txKeyMap.put(AttributeName.TXID.toString(), new AttributeValue(this.txId));
            this.txKey = Collections.unmodifiableMap(txKeyMap);
        } else {
            throw new TransactionException(null, "Either txId or txItem must be specified");
        }

        // Initialize the version
        AttributeValue txVersionVal = this.txItem.get(AttributeName.VERSION.toString());
        if(txVersionVal == null || txVersionVal.getN() == null) {
            throw new TransactionException(this.txId, "Version number is not present in TX record");    
        }
        version = Integer.parseInt(txVersionVal.getN());
        
        // Build the requests structure
        loadRequests();
    }
    
    /**
     * Inserts a new transaction item into the table.  Assumes txKey is already initialized.
     * @return the txItem
     * @throws TransactionException if the transaction already exists
     */
    private Map<String, AttributeValue> insert() {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(AttributeName.STATE.toString(), new AttributeValue(STATE_PENDING));
        item.put(AttributeName.VERSION.toString(), new AttributeValue().withN(Integer.toString(1)));
        item.put(AttributeName.DATE.toString(), txManager.getCurrentTimeAttribute());
        item.putAll(txKey);
        
        Map<String, ExpectedAttributeValue> expectNotExists = new HashMap<String, ExpectedAttributeValue>(2);
        expectNotExists.put(AttributeName.TXID.toString(), new ExpectedAttributeValue(false));
        expectNotExists.put(AttributeName.STATE.toString(), new ExpectedAttributeValue(false));
        
        PutItemRequest request = new PutItemRequest()
            .withTableName(txManager.getTransactionTableName())
            .withItem(item)
            .withExpected(expectNotExists);
        
        try {
            txManager.getClient().putItem(request);
            return item;
        } catch (ConditionalCheckFailedException e) {
            throw new TransactionException("Failed to create new transaction with id " + txId, e);
        }
    }
    
    /**
     * Fetches this transaction item from the tx table.  Uses consistent read.
     * 
     * @return the latest copy of the transaction item, or null if it has been completed (and deleted) 
     */
    private Map<String, AttributeValue> get() {
        GetItemRequest getRequest = new GetItemRequest()
            .withTableName(txManager.getTransactionTableName())
            .withKey(txKey)
            .withConsistentRead(true);
        return txManager.getClient().getItem(getRequest).getItem();
    }
    
    /**
     * Gets the version of the transaction image currently loaded.  Useful for determining if the item has changed when committing the transaction.
     */
    public int getVersion() {
        return version;
    }
    
    /**
     * Determines whether the record is a valid transaction item.  Useful because backups of items in the transaction are 
     * saved in the same table as the transaction item.
     * 
     * @param txItem
     * @return
     */
    public static boolean isTransactionItem(Map<String, AttributeValue> txItem) {
        if(txItem == null) {
            throw new TransactionException(null, "txItem must not be null");
        }
        
        if(! txItem.containsKey(AttributeName.TXID.toString())) {
            return false;
        }
        
        if(txItem.get(AttributeName.TXID.toString()).getS() == null) {
            return false;
        }
        
        return true;
    }
    
    public long getLastUpdateTimeMillis() {
        AttributeValue requestsVal = txItem.get(AttributeName.DATE.toString());
        if(requestsVal == null || requestsVal.getN() == null) {
            throw new TransactionAssertionException(txId, "Expected date attribute to be defined");
        }
        
        try {
            double date = Double.parseDouble(requestsVal.getN());
            return (long)(date * 1000.0);
        } catch (NumberFormatException e) {
            throw new TransactionException("Excpected valid date attribute, was: " + requestsVal.getN(), e);
        }
    }
    
    /*
     * For adding to and maintaining the requests within the transactions
     */
    
    /**
     * Returns the requests in the tx item, sorted by table, then item primary key.  If a lock request was overwritten by 
     * a write, or a lock happened after a write, that lock will not be returned in this list.  
     * 
     * @param txItem
     * @return
     */
    public ArrayList<Request> getRequests() {
        ArrayList<Request> requests = new ArrayList<Request>();
        for(Map.Entry<String, HashMap<ImmutableKey, Request>> tableRequests : requestsMap.entrySet()) {
            for(Map.Entry<ImmutableKey, Request> keyRequests : tableRequests.getValue().entrySet()) {
                requests.add(keyRequests.getValue());
            }
        }
        return requests;
    }
    
    /** 
     * Returns the Request for this table and key, or null if that item is not in this transaction.
     * 
     * @param tableName
     * @param key
     * @return
     */
    public Request getRequestForKey(String tableName, Map<String, AttributeValue> key) {
        HashMap<ImmutableKey, Request> tableRequests = requestsMap.get(tableName);
        
        if(tableRequests != null) {
            Request request = tableRequests.get(new ImmutableKey(key));
            if(request != null) {
                return request;
            }
        }
        
        return null;
    }
    
    /**
     * Adds a request object (input param) to the transaction item.  Enforces that request are unique for a given table name and primary key.
     * Doesn't let you do more than one write per item.  However you can upgrade a read lock to a write.
     * @param callerRequest 
     * @throws ConditionalCheckFailedException if the tx item changed out from under us.  If you get this you must throw this TransactionItem away.
     * @throws DuplicateRequestException If you get this you do not need to throw away the item.
     * @throws InvalidRequestException If the request would add too much data to the transaction
     * @return true if the request was added, false if it didn't need to be added (because it was a duplicate lock request)
     */
    public synchronized boolean addRequest(Request callerRequest) throws ConditionalCheckFailedException, DuplicateRequestException {
        // 1. Ensure the request is unique (modifies the internal data structure if it is unique)
        //    However, do not not short circuit.  If we're doing a read in a resumed transaction, it's important to ensure we're returning
        //    any writes that happened before. 
        addRequestToMap(callerRequest);
        
        callerRequest.setRid(version);
        
        // 2. Write request to transaction item
        ByteBuffer requestBytes = Request.serialize(txId, callerRequest);
        AttributeValueUpdate txItemUpdate = new AttributeValueUpdate()
            .withAction(AttributeAction.ADD)
            .withValue(new AttributeValue().withBS(Arrays.asList(requestBytes)));
        
        Map<String, AttributeValueUpdate> txItemUpdates = new HashMap<String, AttributeValueUpdate>();
        txItemUpdates.put(AttributeName.REQUESTS.toString(), txItemUpdate);    
        txItemUpdates.put(AttributeName.VERSION.toString(), new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(new AttributeValue().withN("1")));
        txItemUpdates.put(AttributeName.DATE.toString(), new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(txManager.getCurrentTimeAttribute()));
        
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
        expected.put(AttributeName.STATE.toString(), new ExpectedAttributeValue(new AttributeValue(STATE_PENDING)));
        expected.put(AttributeName.VERSION.toString(), new ExpectedAttributeValue(new AttributeValue().withN(Integer.toString(version))));
        
        UpdateItemRequest txItemUpdateRequest = new UpdateItemRequest()
            .withTableName(txManager.getTransactionTableName())
            .withKey(txKey)
            .withExpected(expected)
            .withReturnValues(ReturnValue.ALL_NEW)
            .withAttributeUpdates(txItemUpdates);
        
        try {
            txItem = txManager.getClient().updateItem(txItemUpdateRequest).getAttributes();
            int newVersion = Integer.parseInt(txItem.get(AttributeName.VERSION.toString()).getN());
            txAssert(newVersion == version + 1, txId, "Unexpected version number from update result");
            version = newVersion;
        } catch (AmazonServiceException e) {
            if("ValidationException".equals(e.getErrorCode())) {
                removeRequestFromMap(callerRequest);
                throw new InvalidRequestException("The amount of data in the transaction cannot exceed the DynamoDB item size limit", 
                    txId, callerRequest.getTableName(), callerRequest.getKey(txManager), callerRequest);
            } else {
                throw e;
            }
        }
        return true;
    }
    
    /**
     * Reads the requests in the loaded txItem and adds them to the map of table -> key.
     */
    private void loadRequests() {
        AttributeValue requestsVal = txItem.get(AttributeName.REQUESTS.toString());
        List<ByteBuffer> rawRequests = (requestsVal != null && requestsVal.getBS() != null) ? requestsVal.getBS() : new ArrayList<ByteBuffer>(0);
        
        for(ByteBuffer rawRequest : rawRequests) {
            Request request = Request.deserialize(txId, rawRequest);
            // TODO don't make strings out of the PK all the time, also dangerous if behavior of toString changes!
            addRequestToMap(request);
        }
    }
    
    /**
     * Adds the request to the internal map structure if it doesn't already exist. If there is a write and a read to the same item,
     * only the write will appear in this map. 
     * 
     * @param request
     * @throws DuplicateRequestException if there are multiple write operations to the same item.
     * @return true if the request was added, false if not (isn't added if it's a read where there is already a write)
     */
    private boolean addRequestToMap(Request request) throws DuplicateRequestException {
        Map<String, AttributeValue> key = request.getKey(txManager);
        ImmutableKey immutableKey = new ImmutableKey(key);
        
        HashMap<ImmutableKey, Request> pkToRequestMap = requestsMap.get(request.getTableName());
        
        if(pkToRequestMap == null) {
            pkToRequestMap = new HashMap<ImmutableKey, Request>();
            requestsMap.put(request.getTableName(), pkToRequestMap);
        }
        
        Request existingRequest = pkToRequestMap.get(immutableKey);
        if(existingRequest != null) {
            if(request instanceof GetItem) {
                return false;
            }
            
            if(existingRequest instanceof GetItem) {
                // ok to overwrite a lock with a write
            } else {
                throw new DuplicateRequestException(txId, request.getTableName(), key.toString());    
            }
        }
        
        pkToRequestMap.put(immutableKey, request);
        return true;
    }
    
    /**
     * Really should only be used in the catch of addRequest 
     * 
     * @param request
     */
    private void removeRequestFromMap(Request request) {
        // It's okay to leave empty maps around
        ImmutableKey key = new ImmutableKey(request.getKey(txManager));
        requestsMap.get(request.getTableName()).remove(key);
    }
    
    /*
     * For saving and loading old item images 
     */
    
    /**
     * Saves the old copy of the item.  Does not mutate the item, unless an exception is thrown.
     * 
     * @param item
     * @param rid
     */
    public void saveItemImage(Map<String, AttributeValue> item, int rid) {
        txAssert(! item.containsKey(AttributeName.APPLIED.toString()), txId, "The transaction has already applied this item image, it should not be saving over the item image with it");
        
        AttributeValue existingTxId = item.put(AttributeName.TXID.toString(), new AttributeValue(txId));
        if(existingTxId != null && ! txId.equals(existingTxId.getS())) {
            throw new TransactionException(txId, "Items in transactions may not contain the attribute named " + AttributeName.TXID.toString());
        }
        
        // Don't save over the already saved item.  Prevents us from saving the applied image instead of the previous image in the case
        // of a re-drive.
        // If we want to be extremely paranoid, we could expect every attribute to be set exactly already in a second write step, and assert
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>(1);
        expected.put(AttributeName.IMAGE_ID.toString(), new ExpectedAttributeValue().withExists(false));
        
        AttributeValue existingImageId = item.put(AttributeName.IMAGE_ID.toString(), new AttributeValue(txId + "#" + rid));
        if(existingImageId != null) {
            throw new TransactionException(txId, "Items in transactions may not contain the attribute named " + AttributeName.IMAGE_ID.toString() + ", value was already " + existingImageId); 
        }
        
        // TODO failures?  Size validation?
        try {
            txManager.getClient().putItem(new PutItemRequest()
                .withTableName(txManager.getItemImageTableName())
                .withExpected(expected)
                .withItem(item));
        } catch (ConditionalCheckFailedException e) {
            // Already was saved
        }
        
        // do not mutate the item for the customer unless if there aren't exceptions
        item.remove(AttributeName.IMAGE_ID.toString());
    }
    
    /**
     * Retrieves the old copy of the item, with any item image saving specific attributes removed
     * 
     * @param rid
     * @return
     */
    public Map<String, AttributeValue> loadItemImage(int rid) {
        txAssert(rid > 0, txId, "Expected rid > 0");
        
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>(1);
        key.put(AttributeName.IMAGE_ID.toString(), new AttributeValue(txId + "#" + rid));
        
        Map<String, AttributeValue> item = txManager.getClient().getItem(new GetItemRequest()
            .withTableName(txManager.getItemImageTableName())
            .withKey(key)
            .withConsistentRead(true)).getItem();
        
        if(item != null) {
            item.remove(AttributeName.IMAGE_ID.toString());
        }
        
        return item;
    }
    
    /**
     * Deletes the old version of the item.  Item images are immutable - it's just create + delete, so there is no need for
     * concurrent modification checks.
     * 
     * @param rid
     */
    public void deleteItemImage(int rid) {
        txAssert(rid > 0, txId, "Expected rid > 0");
        
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>(1);
        key.put(AttributeName.IMAGE_ID.toString(), new AttributeValue(txId + "#" + rid));
        
        txManager.getClient().deleteItem(new DeleteItemRequest()
            .withTableName(txManager.getItemImageTableName())
            .withKey(key));
    }
    
    /*
     * For changing the state of the transaction
     */

    /**
     * Marks the transaction item as either COMMITTED or ROLLED_BACK, but only if it was in the PENDING state.
     * It will also condition on the expected version. 
     * 
     * @param targetState
     * @param expectedVersion 
     * @throws ConditionalCheckFailedException if the transaction doesn't exist, isn't PENDING, is finalized, 
     *         or the expected version doesn't match (if specified)  
     */
    public void finish(final State targetState, final int expectedVersion) throws ConditionalCheckFailedException {
        txAssert(State.COMMITTED.equals(targetState) || State.ROLLED_BACK.equals(targetState),"Illegal state in finish(): " + targetState, "txItem", txItem);
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>(2);
        expected.put(AttributeName.STATE.toString(), new ExpectedAttributeValue().withValue(new AttributeValue().withS(STATE_PENDING)));
        expected.put(AttributeName.FINALIZED.toString(), new ExpectedAttributeValue().withExists(false));
        expected.put(AttributeName.VERSION.toString(), new ExpectedAttributeValue().withValue(new AttributeValue().withN(Integer.toString(expectedVersion))));
        
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put(AttributeName.STATE.toString(), new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue(stateToString(targetState))));
        updates.put(AttributeName.DATE.toString(), new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(txManager.getCurrentTimeAttribute()));
        
        UpdateItemRequest finishRequest = new UpdateItemRequest()
            .withTableName(txManager.getTransactionTableName())
            .withKey(txKey)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)
            .withExpected(expected);
        
        UpdateItemResult finishResult = txManager.getClient().updateItem(finishRequest);
        txItem = finishResult.getAttributes();
        if(txItem == null) {
            throw new TransactionAssertionException(txId, "Unexpected null tx item after committing " + targetState);
        }
    }
    
    /**
     * Completes a transaction by marking its "Finalized" attribute.  This leaves the completed transaction item around
     * so that the party who created the transaction can see whether it was completed or rolled back.  They can then either 
     * delete the transaction record when they're done, or they can run a sweeper process to go and delete the completed transactions
     * later on. 
     * 
     * @param expectedCurrentState
     * @throws ConditionalCheckFailedException if the transaction is completed, doesn't exist anymore, or even if it isn't committed or rolled back  
     */
    public void complete(final State expectedCurrentState) throws ConditionalCheckFailedException {
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>(2);
        
        if(State.COMMITTED.equals(expectedCurrentState)) {
            expected.put(AttributeName.STATE.toString(), new ExpectedAttributeValue(new AttributeValue(STATE_COMMITTED)));
        } else if(State.ROLLED_BACK.equals(expectedCurrentState)) {
            expected.put(AttributeName.STATE.toString(), new ExpectedAttributeValue(new AttributeValue(STATE_ROLLED_BACK)));
        } else {
            throw new TransactionAssertionException(txId, "Illegal state in finish(): " + expectedCurrentState + " txItem " + txItem);
        }
        
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put(AttributeName.FINALIZED.toString(), new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue(Transaction.BOOLEAN_TRUE_ATTR_VAL)));
        updates.put(AttributeName.DATE.toString(), new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(txManager.getCurrentTimeAttribute()));
        
        UpdateItemRequest completeRequest = new UpdateItemRequest()
            .withTableName(txManager.getTransactionTableName())
            .withKey(txKey)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)
            .withExpected(expected);
        
        txItem = txManager.getClient().updateItem(completeRequest).getAttributes();
    }
    
    /**
     * Deletes the tx item, only if it was in the "finalized" state.
     * 
     * @throws ConditionalCheckFailedException if the item does not exist or is not finalized
     */
    public void delete() throws ConditionalCheckFailedException {
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>(1);
        expected.put(AttributeName.FINALIZED.toString(), new ExpectedAttributeValue().withValue(new AttributeValue(Transaction.BOOLEAN_TRUE_ATTR_VAL)));
        
        DeleteItemRequest completeRequest = new DeleteItemRequest()
            .withTableName(txManager.getTransactionTableName())
            .withKey(txKey)
            .withExpected(expected);
        txManager.getClient().deleteItem(completeRequest);
    }
    
    public boolean isCompleted() {
        boolean isCompleted = txItem.containsKey(AttributeName.FINALIZED.toString());
        if(isCompleted) {
            txAssert(State.COMMITTED.equals(getState()) || State.ROLLED_BACK.equals(getState()), txId,
                "Unexpected terminal state for completed transaction", "state", getState());
        }
        return isCompleted;
    }
    
    /**
     * For unit testing only
     * @return
     */
    protected Map<String, HashMap<ImmutableKey, Request>> getRequestMap() {
        return requestsMap;
    }

    public enum State {
        PENDING,
        COMMITTED,
        ROLLED_BACK
    }
    
    /**
     * Returns the state of the transaction item.  Keep in mind that the current state is never truly known until you try to perform an action,
     * so be careful with how you use this information.
     * 
     * @return
     */
    public State getState() {
        AttributeValue stateVal = txItem.get(AttributeName.STATE.toString());
        String txState = (stateVal != null) ? stateVal.getS() : null;
        
        if(STATE_COMMITTED.equals(txState)) {
            return State.COMMITTED;
        } else if(STATE_ROLLED_BACK.equals(txState)) {
            return State.ROLLED_BACK;
        } else if(STATE_PENDING.equals(txState)) {
            return State.PENDING;
        } else {
            throw new TransactionAssertionException(txId, "Unrecognized transaction state: " + txState);
        }
    }
    
    public static String stateToString(State state) {
        switch (state) {
            case PENDING:
                return STATE_PENDING;
            case COMMITTED: 
                return STATE_COMMITTED;
            case ROLLED_BACK:
                return STATE_ROLLED_BACK;
            default:
                throw new TransactionAssertionException(null, "Unrecognized transaction state: " + state); 
        }
    }
    
}
