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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.transactions.Request;
import com.amazonaws.services.dynamodbv2.transactions.Request.DeleteItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.GetItem;
import com.amazonaws.services.dynamodbv2.transactions.Transaction;
import com.amazonaws.services.dynamodbv2.transactions.Transaction.AttributeName;
import com.amazonaws.services.dynamodbv2.transactions.TransactionItem;
import com.amazonaws.services.dynamodbv2.transactions.TransactionManager;
import com.amazonaws.services.dynamodbv2.transactions.FailingAmazonDynamoDBClient.FailedYourRequestException;
import com.amazonaws.services.dynamodbv2.transactions.Transaction.IsolationLevel;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.DuplicateRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.InvalidRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.ItemNotLockedException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionCommittedException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionNotFoundException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionRolledBackException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.UnknownCompletedTransactionException;
import com.amazonaws.services.dynamodbv2.util.ImmutableKey;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;

public class TransactionsIntegrationTest {
    
    protected static final FailingAmazonDynamoDBClient dynamodb;
    private static final String HASH_TABLE_NAME = "TransactionsIntegrationTest_Hash";
    private static final String HASH_RANGE_TABLE_NAME = "TransactionsIntegrationTest_HashRange";
    private static final String LOCK_TABLE_NAME = "TransactionsIntegrationTest_Transactions";
    private static final String IMAGES_TABLE_NAME = "TransactionsIntegrationTest_ItemImages";
    private static final String TABLE_NAME_SUFFIX = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());
    private static final String DYNAMODB_ENDPOINT = "http://dynamodb.us-west-2.amazonaws.com";
    
    private Map<String, AttributeValue> key0;
    private Map<String, AttributeValue> item0;
    
    private final TransactionManager manager;
    
    protected static String getLockTableName() {
        return LOCK_TABLE_NAME + "_" + TABLE_NAME_SUFFIX;
    }
    
    protected static String getImagesTableName() {
        return IMAGES_TABLE_NAME + "_" + TABLE_NAME_SUFFIX;
    }
    
    protected static String getHashTableName() {
        return HASH_TABLE_NAME + "_" + TABLE_NAME_SUFFIX;
    }
    
    protected static String getHashRangeTableName() {
        return HASH_RANGE_TABLE_NAME + "_" + TABLE_NAME_SUFFIX;
    }
    
    protected Map<String, AttributeValue> newKey(String tableName) {
        
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withS("val_" + Math.random()));
        if(getHashTableName().equals(tableName)) {
            // no-op
        } else if (getHashRangeTableName().equals(tableName)) {
            key.put("RangeAttr", new AttributeValue().withN(new Double(Math.random()).toString()));
        } else {
            throw new IllegalArgumentException();
        }
        return key;
    }
    
    @BeforeClass
    public static void createTables() throws InterruptedException {
        
        try {
            CreateTableRequest createHash = new CreateTableRequest()
                .withTableName(getHashTableName())
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
            dynamodb.createTable(createHash);
        } catch (ResourceInUseException e) {
            System.err.println("Warning: " + getHashTableName() + " was already in use");
        }
        
        try {
            TransactionManager.verifyOrCreateTransactionTable(dynamodb, getLockTableName(), 10L, 10L, 5L * 60);
            TransactionManager.verifyOrCreateTransactionImagesTable(dynamodb, getImagesTableName(), 10L, 10L, 5L * 60);
        } catch (ResourceInUseException e) {
            System.err.println("Warning: " + getHashTableName() + " was already in use");
        }
        
        waitForTableToBecomeAvailable(getHashTableName());
        waitForTableToBecomeAvailable(getLockTableName());
        waitForTableToBecomeAvailable(getImagesTableName());
    }
    
    @Before
    public void setup() {
        dynamodb.reset();
        Transaction t = manager.newTransaction();
        key0 = newKey(getHashTableName());
        item0 = new HashMap<String, AttributeValue>(key0);
        item0.put("s_someattr", new AttributeValue("val"));
        item0.put("ss_otherattr", new AttributeValue().withSS("one", "two"));
        Map<String, AttributeValue> putResult = t.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item0)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertNull(putResult);
        t.commit();
        assertItemNotLocked(getHashTableName(), key0, item0, true);
    }
    
    @After
    public void teardown() {
        dynamodb.reset();
        Transaction t = manager.newTransaction();
        t.deleteItem(new DeleteItemRequest().withTableName(getHashTableName()).withKey(key0));
        t.commit();
        assertItemNotLocked(getHashTableName(), key0, false);
    }
    
    private static void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            DescribeTableRequest request = new DescribeTableRequest()
                    .withTableName(tableName);
            TableDescription tableDescription = dynamodb.describeTable(
                    request).getTable();
            String tableStatus = tableDescription.getTableStatus();
            System.out.println("  - current state: " + tableStatus);
            if (tableStatus.equals(TableStatus.ACTIVE.toString()))
                return;
            try { Thread.sleep(1000 * 10); } catch (Exception e) { }
        }
        throw new RuntimeException("Table " + tableName + " never went active");
    }
    
    static {
        AWSCredentials credentials;
        try {
            credentials = new PropertiesCredentials(
                    TransactionsIntegrationTest.class.getResourceAsStream("AwsCredentials.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        dynamodb = new FailingAmazonDynamoDBClient(credentials);
        dynamodb.setEndpoint(DYNAMODB_ENDPOINT);
    }

    public TransactionsIntegrationTest() throws IOException {
        manager = new TransactionManager(dynamodb, getLockTableName(), getImagesTableName());
    }
    
    @Test
    public void phantomItemFromDelete() {
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Transaction transaction = manager.newTransaction();
        DeleteItemRequest deleteRequest = new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1);
        transaction.deleteItem(deleteRequest);
        assertItemLocked(getHashTableName(), key1, transaction.getId(), true, false);
        transaction.rollback();
        assertItemNotLocked(getHashTableName(), key1, false);
        transaction.delete(Long.MAX_VALUE);
    }
    
    /*
     * GetItem tests
     */
    
    @Test
    public void lockItem() {
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Transaction t1 = manager.newTransaction();
        Transaction t2 = manager.newTransaction();
        
        DeleteItemRequest deleteRequest = new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1);
        
        GetItemRequest lockRequest = new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1);
        
        Map<String, AttributeValue> getResult = t1.getItem(lockRequest).getItem();
        
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false); // we're not applying locks
        assertNull(getResult);

        Map<String, AttributeValue> deleteResult = t2.deleteItem(deleteRequest).getAttributes();
        assertItemLocked(getHashTableName(), key1, t2.getId(), true, false); // we're not applying deletes either
        assertNull(deleteResult); // return values is null in the request
        
        t2.commit();
        
        try {
            t1.commit();
            fail();
        } catch (TransactionRolledBackException e) { }
        
        t1.delete(Long.MAX_VALUE);
        t2.delete(Long.MAX_VALUE);
        
        assertItemNotLocked(getHashTableName(), key1, false);
    }
    
    @Test
    public void lock2Items() {
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        
        Transaction t0 = manager.newTransaction();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("something", new AttributeValue("val"));
        Map<String, AttributeValue> putResult = t0.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertNull(putResult);
        
        t0.commit();
        
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> getResult1 = t1.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1)).getItem();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), false, false);
        assertEquals(item1, getResult1);
        
        Map<String, AttributeValue> getResult2 = t1.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2)).getItem();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), false, false);
        assertItemLocked(getHashTableName(), key2, t1.getId(), true, false);
        assertNull(getResult2);
        
        t1.commit();
        t1.delete(Long.MAX_VALUE);
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
        assertItemNotLocked(getHashTableName(), key2, false);
    }
    
    @Test
    public void getItemWithDelete() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> getResult1 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key0)).getItem();
        assertEquals(getResult1, item0);
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        t1.deleteItem(new DeleteItemRequest().withTableName(getHashTableName()).withKey(key0));
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        Map<String, AttributeValue> getResult2 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key0)).getItem();
        assertNull(getResult2);
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        t1.commit();
    }
    
    @Test
    public void getFilterAttributesToGet() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
        item1.put("s_someattr", item0.get("s_someattr"));
        
        Map<String, AttributeValue> getResult1 = t1.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withAttributesToGet("s_someattr", "notexists")
            .withKey(key0)).getItem();
        assertEquals(item1, getResult1);
        assertItemLocked(getHashTableName(), key0, t1.getId(), false, false);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, item0, true);
    }
    
    @Test
    public void getItemNotExists() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        
        Map<String, AttributeValue> getResult1 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertNull(getResult1);
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false);
        
        Map<String, AttributeValue> getResult2 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertNull(getResult2);
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, false);
    }
    
    @Test
    public void getItemAfterPutItemInsert() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        
        Map<String, AttributeValue> getResult1 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertNull(getResult1);
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false);
        
        Map<String, AttributeValue> putResult1 = t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertNull(putResult1);
        
        Map<String, AttributeValue> getResult2 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertEquals(getResult2, item1);
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void getItemAfterPutItemOverwrite() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("wef"));
        
        Map<String, AttributeValue> getResult1 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key0)).getItem();
        assertEquals(getResult1, item0);
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        Map<String, AttributeValue> putResult1 = t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        assertEquals(putResult1, item0);
        
        Map<String, AttributeValue> getResult2 = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key0)).getItem();
        assertEquals(getResult2, item1);
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, item1, true);
    }
    
    @Test
    public void getItemAfterPutItemInsertInResumedTx() {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> applyAndKeepLock(Request request, Map<String, AttributeValue> lockedItem) {
                throw new FailedYourRequestException();
            }  
        };
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        
        try {
            // This Put needs to fail in apply
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item1)
                .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
            fail();
        } catch (FailedYourRequestException e) {}
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false);
        
        // second copy of same tx
        Map<String, AttributeValue> getResult1 = t2.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertEquals(getResult1, item1);
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        t2.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void getItemThenPutItemInResumedTxThenGetItem() {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> applyAndKeepLock(Request request, Map<String, AttributeValue> lockedItem) {
                if(request instanceof GetItem || request instanceof DeleteItem) {
                    return super.applyAndKeepLock(request, lockedItem);
                }
                throw new FailedYourRequestException();
            }  
        };
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        
        // Get a read lock in t2
        Map<String, AttributeValue> getResult1 = t2.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertNull(getResult1);
        assertItemLocked(getHashTableName(), key1, null, t1.getId(), true, false);
        
        // Begin a PutItem in t1, but fail apply
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item1)
                .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
            fail();
        } catch (FailedYourRequestException e) {}
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false);
        
        // Read again in the non-failing copy of the transaction
        Map<String, AttributeValue> getResult2 = t2.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        t2.commit();
        assertEquals(item1, getResult2);
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void getThenUpdateNewItem() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("didn't exist"));
        
        Map<String, AttributeValueUpdate> updates1 = new HashMap<String, AttributeValueUpdate>();
        updates1.put("asdf", new AttributeValueUpdate(new AttributeValue("didn't exist"), AttributeAction.PUT));
        
        Map<String, AttributeValue> getResult = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key1)).getItem();
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, false);
        assertNull(getResult);
        
        Map<String, AttributeValue> updateResult = t1.updateItem(new UpdateItemRequest().withTableName(getHashTableName()).withKey(key1)
                .withAttributeUpdates(updates1).withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertEquals(item1, updateResult);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void getThenUpdateExistingItem() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> item0a = new HashMap<String, AttributeValue>(item0);
        item0a.put("wef", new AttributeValue("new attr"));
        
        Map<String, AttributeValueUpdate> updates1 = new HashMap<String, AttributeValueUpdate>();
        updates1.put("wef", new AttributeValueUpdate(new AttributeValue("new attr"), AttributeAction.PUT));
        
        Map<String, AttributeValue> getResult = t1.getItem(new GetItemRequest().withTableName(getHashTableName()).withKey(key0)).getItem();
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        assertEquals(item0, getResult);
        
        Map<String, AttributeValue> updateResult = t1.updateItem(new UpdateItemRequest().withTableName(getHashTableName()).withKey(key0)
                .withAttributeUpdates(updates1).withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        assertItemLocked(getHashTableName(), key0, item0a, t1.getId(), false, true);
        assertEquals(item0a, updateResult);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, item0a, true);
    }
    
    @Test
    public void getItemUncommittedInsert() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1), IsolationLevel.UNCOMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item1, item);
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        t1.rollback();
    }
    
    @Test
    public void getItemUncommittedDeleted() {
        Transaction t1 = manager.newTransaction();
        
        t1.deleteItem(new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0));
        
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0), IsolationLevel.UNCOMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item0, item);
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        t1.rollback();
    }
    
    @Test
    public void getItemCommittedInsert() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1), IsolationLevel.COMMITTED).getItem();
        assertNull(item);
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        t1.rollback();
    }
    
    @Test
    public void getItemCommittedDeleted() {
        Transaction t1 = manager.newTransaction();
        
        t1.deleteItem(new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0));
        
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0), IsolationLevel.COMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item0, item);
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        t1.rollback();
    }
    
    @Test
    public void getItemCommittedUpdated() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("asdf")));
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("asdf"));
        
        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withAttributeUpdates(updates)
            .withKey(key0));
        
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        
        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0), IsolationLevel.COMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item0, item);
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        
        t1.commit();
        
        item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0), IsolationLevel.COMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item1, item);
    }
    
    @Test
    public void getItemCommittedUpdatedAndApplied() {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected void doCommit() {
                //Skip cleaning up the transaction so we can validate reading.
            }
        };

        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("asdf")));
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("asdf"));

        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withAttributeUpdates(updates)
            .withKey(key0));

        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);

        t1.commit();

        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0), IsolationLevel.COMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item1, item);
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
    }
    
    @Test
    public void getItemCommittedMissingImage() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("asdf")));
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("asdf"));

        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withAttributeUpdates(updates)
            .withKey(key0));

        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        
        dynamodb.getRequestsToTreatAsDeleted.add(new GetItemRequest()
            .withTableName(manager.getItemImageTableName())
            .addKeyEntry(AttributeName.IMAGE_ID.toString(), new AttributeValue(t1.getTxItem().txId + "#" + 1))
            .withConsistentRead(true));
        
        try {
            manager.getItem(new GetItemRequest()
                .withTableName(getHashTableName())
                .withKey(key0), IsolationLevel.COMMITTED).getItem();
            fail("Should have thrown an exception.");
        } catch (TransactionException e) {
            assertEquals("null - Ran out of attempts to get a committed image of the item", e.getMessage());
        }
    }
    
    @Test
    public void getItemCommittedConcurrentCommit() {
        //Test reading an item while simulating another transaction committing concurrently.
        //To do this we skip cleanup, make the item image appear to be deleted,
        //and then make the reader get the uncommitted version of the transaction 
        //row for the first read and then actual updated version for later reads.
        
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected void doCommit() {
                //Skip cleaning up the transaction so we can validate reading.
            }
        };
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("asdf")));
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("asdf"));

        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withAttributeUpdates(updates)
            .withKey(key0));

        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        
        GetItemRequest txItemRequest = new GetItemRequest()
            .withTableName(manager.getTransactionTableName())
            .addKeyEntry(AttributeName.TXID.toString(), new AttributeValue(t1.getTxItem().txId))
            .withConsistentRead(true);
        
        //Save the copy of the transaction before commit. 
        GetItemResult uncommittedTransaction = dynamodb.getItem(txItemRequest);
        
        t1.commit();
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        
        dynamodb.getRequestsToStub.put(txItemRequest, new LinkedList<GetItemResult>(Collections.singletonList(uncommittedTransaction)));
        //Stub out the image so it appears deleted
        dynamodb.getRequestsToTreatAsDeleted.add(new GetItemRequest()
            .withTableName(manager.getItemImageTableName())
            .addKeyEntry(AttributeName.IMAGE_ID.toString(), new AttributeValue(t1.getTxItem().txId + "#" + 1))
            .withConsistentRead(true));
        
        Map<String, AttributeValue> item = manager.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0), IsolationLevel.COMMITTED).getItem();
        assertNoSpecialAttributes(item);
        assertEquals(item1, item);
    }
    
    /*
     * ReturnValues tests
     */
    
    @Test
    public void putItemAllOldInsert() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        
        Map<String, AttributeValue> putResult1 = t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertNull(putResult1);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void putItemAllOldOverwrite() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("wef"));
        
        Map<String, AttributeValue> putResult1 = t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        assertEquals(putResult1, item0);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, item1, true);
    }
    
    @Test
    public void updateItemAllOldInsert() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("wef")));
        
        Map<String, AttributeValue> result1 = t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertNull(result1);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void updateItemAllOldOverwrite() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("wef"));
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("wef")));
        
        Map<String, AttributeValue> result1 = t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        assertEquals(result1, item0);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, item1, true);
    }
    
    @Test
    public void updateItemAllNewInsert() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("asdf", new AttributeValue("wef"));
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("wef")));
        
        Map<String, AttributeValue> result1 = t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertEquals(result1, item1);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
    }
    
    @Test
    public void updateItemAllNewOverwrite() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(item0);
        item1.put("asdf", new AttributeValue("wef"));
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("asdf", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("wef")));
        
        Map<String, AttributeValue> result1 = t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        assertItemLocked(getHashTableName(), key0, item1, t1.getId(), false, true);
        assertEquals(result1, item1);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, item1, true);
    }
    
    @Test
    public void deleteItemAllOldNotExists() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        
        Map<String, AttributeValue> result1 = t1.deleteItem(new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key1, key1, t1.getId(), true, false);
        assertNull(result1);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, false);
    }
    
    @Test
    public void deleteItemAllOldExists() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> result1 = t1.deleteItem(new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key0)
            .withReturnValues(ReturnValue.ALL_OLD)).getAttributes();
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        assertEquals(item0, result1);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key0, false);
    }
    
    /*
     * Transaction isolation and error tests
     */
    
    @Test
    public void conflictingWrites() {
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Transaction t1 = manager.newTransaction();
        Transaction t2 = manager.newTransaction();
        Transaction t3 = manager.newTransaction();
        
        // Finish t1 
        Map<String, AttributeValue> t1Item = new HashMap<String, AttributeValue>(key1);
        t1Item.put("whoami", new AttributeValue("t1"));
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(new HashMap<String, AttributeValue>(t1Item)));
        assertItemLocked(getHashTableName(), key1, t1Item, t1.getId(), true, true);
        
        t1.commit();
        assertItemNotLocked(getHashTableName(), key1, t1Item, true);
        
        // Begin t2
        Map<String, AttributeValue> t2Item = new HashMap<String, AttributeValue>(key1);
        t2Item.put("whoami", new AttributeValue("t2"));
        t2Item.put("t2stuff", new AttributeValue("extra"));
        
        t2.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(new HashMap<String, AttributeValue>(t2Item)));
        assertItemLocked(getHashTableName(), key1, t2Item, t2.getId(), false, true);
        
        // Begin and finish t3
        Map<String, AttributeValue> t3Item = new HashMap<String, AttributeValue>(key1);
        t3Item.put("whoami", new AttributeValue("t3"));
        t3Item.put("t3stuff", new AttributeValue("things"));
        
        t3.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(new HashMap<String, AttributeValue>(t3Item)));
        assertItemLocked(getHashTableName(), key1, t3Item, t3.getId(), false, true);
        
        t3.commit();
        
        assertItemNotLocked(getHashTableName(), key1, t3Item, true);
        
        // Ensure t2 rolled back
        try {
            t2.commit();
            fail();
        } catch (TransactionRolledBackException e) { }
        
        t1.delete(Long.MAX_VALUE);
        t2.delete(Long.MAX_VALUE);
        t3.delete(Long.MAX_VALUE);
        
        assertItemNotLocked(getHashTableName(), key1, t3Item, true);
    }
    
    @Test
    public void failValidationInApply() {
        Map<String, AttributeValue> key = newKey(getHashTableName());
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("FooAttribute", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("Bar")));
        
        Transaction t1 = manager.newTransaction();
        Transaction t2 = manager.newTransaction();
        
        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key)
            .withAttributeUpdates(updates));
        
        assertItemLocked(getHashTableName(), key, t1.getId(), true, true);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key, true);
        
        updates.put("FooAttribute", new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(new AttributeValue().withN("1")));
     
        try {
            t2.updateItem(new UpdateItemRequest()
                .withTableName(getHashTableName())
                .withKey(key)
                .withAttributeUpdates(updates));
            fail();
        } catch (AmazonServiceException e) {
            assertEquals("ValidationException", e.getErrorCode());
            assertTrue(e.getMessage().contains("Type mismatch for attribute"));
        }
        
        assertItemLocked(getHashTableName(), key, t2.getId(), false, false);
        
        try {
            t2.commit();
            fail();
        } catch (AmazonServiceException e) {
            assertEquals("ValidationException", e.getErrorCode());
            assertTrue(e.getMessage().contains("Type mismatch for attribute"));
        }
        
        assertItemLocked(getHashTableName(), key, t2.getId(), false, false);
        
        t2.rollback();
        
        assertItemNotLocked(getHashTableName(), key, true);
        
        t1.delete(Long.MAX_VALUE);
        t2.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void useCommittedTransaction() {
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Transaction t1 = manager.newTransaction();
        t1.commit();
        
        DeleteItemRequest deleteRequest = new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1);
        
        try {
            t1.deleteItem(deleteRequest);
            fail();
        } catch (TransactionCommittedException e) { }
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        try {
            t1.deleteItem(deleteRequest);
            fail();
        } catch (TransactionCommittedException e) { }
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        try {
            t2.rollback();
            fail();
        } catch (TransactionCommittedException e) { }
        
        t2.delete(Long.MAX_VALUE);
        t1.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void useRolledBackTransaction() {
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Transaction t1 = manager.newTransaction();
        t1.rollback();
        
        DeleteItemRequest deleteRequest = new DeleteItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1);
        
        try {
            t1.deleteItem(deleteRequest);
            fail();
        } catch (TransactionRolledBackException e) { }
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        try {
            t1.deleteItem(deleteRequest);
            fail();
        } catch (TransactionRolledBackException e) { }
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        try {
            t2.commit();
            fail();
        } catch (TransactionRolledBackException e) { }
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        Transaction t3 = manager.resumeTransaction(t1.getId());
        t3.rollback();
        
        Transaction t4 = manager.resumeTransaction(t1.getId());
        
        t2.delete(Long.MAX_VALUE);
        t1.delete(Long.MAX_VALUE);
        
        try {
            t4.deleteItem(deleteRequest);
            fail();
        } catch (TransactionNotFoundException e) { }
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        t3.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void useDeletedTransaction() {
        Transaction t1 = manager.newTransaction();
        Transaction t2 = manager.resumeTransaction(t1.getId());
        t1.commit();
        t1.delete(Long.MAX_VALUE);
        
        try {
            t2.commit();
            fail();
        } catch (UnknownCompletedTransactionException e) { }
        
        t2.delete(Long.MAX_VALUE);
        
    }
    
    @Test
    public void driveCommit() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue> (key1);
        item.put("attr", new AttributeValue("original"));
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item));
        
        t1.commit();
        t1.delete(Long.MAX_VALUE);
        
        assertItemNotLocked(getHashTableName(), key1, item, true);
        assertItemNotLocked(getHashTableName(), key2, false);
        
        Transaction t2 = manager.newTransaction();
        
        item.put("attr2", new AttributeValue("new"));
        t2.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item));
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2));
        
        assertItemLocked(getHashTableName(), key1, item, t2.getId(), false, true);
        assertItemLocked(getHashTableName(), key2, key2, t2.getId(), true, false);
        
        Transaction commitFailingTransaction = new Transaction(t2.getId(), manager, false) {
            @Override
            protected void unlockItemAfterCommit(Request request) {
                throw new FailedYourRequestException();
            }
        };
        
        try {
            commitFailingTransaction.commit();
            fail();
        } catch (FailedYourRequestException e) { }
        
        assertItemLocked(getHashTableName(), key1, item, t2.getId(), false, true);
        assertItemLocked(getHashTableName(), key2, key2, t2.getId(), true, false);
        
        t2.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item, true);
        assertItemNotLocked(getHashTableName(), key2, false);
        
        commitFailingTransaction.commit();
        
        t2.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void driveRollback() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue> (key2);
        item1.put("attr2", new AttributeValue("original2"));
        
        Map<String, AttributeValue> key3 = newKey(getHashTableName());
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1));
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item2));
        
        t1.commit();
        t1.delete(Long.MAX_VALUE);
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
        assertItemNotLocked(getHashTableName(), key2, item2, true);
        
        Transaction t2 = manager.newTransaction();
        
        Map<String, AttributeValue> item1a = new HashMap<String, AttributeValue> (item1);
        item1a.put("attr1", new AttributeValue("new1"));
        item1a.put("attr2", new AttributeValue("new1"));
       
        t2.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1a));
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2));
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key3));
        
        assertItemLocked(getHashTableName(), key1, item1a, t2.getId(), false, true);
        assertItemLocked(getHashTableName(), key2, item2, t2.getId(), false, false);
        assertItemLocked(getHashTableName(), key3, key3, t2.getId(), true, false);
        
        Transaction rollbackFailingTransaction = new Transaction(t2.getId(), manager, false) {
            @Override
            protected void rollbackItemAndReleaseLock(Request request) {
                throw new FailedYourRequestException();
            }
        };
        
        try {
            rollbackFailingTransaction.rollback();
            fail();
        } catch (FailedYourRequestException e) { }
        
        assertItemLocked(getHashTableName(), key1, item1a, t2.getId(), false, true);
        assertItemLocked(getHashTableName(), key2, item2, t2.getId(), false, false);
        assertItemLocked(getHashTableName(), key3, key3, t2.getId(), true, false);
        
        t2.rollback();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
        assertItemNotLocked(getHashTableName(), key2, item2, true);
        assertItemNotLocked(getHashTableName(), key3, false);
        
        rollbackFailingTransaction.rollback();
        
        t2.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void rollbackCompletedTransaction() {
        Transaction t1 = manager.newTransaction();
        Transaction rollbackFailingTransaction = new Transaction(t1.getId(), manager, false) {
            @Override
            protected void doRollback() {
                throw new FailedYourRequestException();
            }
        };
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(key1));
        assertItemLocked(getHashTableName(), key1, key1, t1.getId(), true, true);
        
        t1.rollback();
        rollbackFailingTransaction.rollback();
    }
    
    @Test
    public void commitCompletedTransaction() {
        Transaction t1 = manager.newTransaction();
        Transaction commitFailingTransaction = new Transaction(t1.getId(), manager, false) {
            @Override
            protected void doCommit() {
                throw new FailedYourRequestException();
            }
        };
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(key1));
        assertItemLocked(getHashTableName(), key1, key1, t1.getId(), true, true);
        
        t1.commit();
        commitFailingTransaction.commit();
    }
    
    @Test
    public void resumePendingTransaction() {
        Transaction t1 = manager.newTransaction();
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        assertItemLocked(getHashTableName(), key2, t1.getId(), true, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        t2.commit();
        
        assertItemNotLocked(getHashTableName(), key1, true);
        assertItemNotLocked(getHashTableName(), key2, false);
        
        assertOldItemImage(t1.getId(), getHashTableName(), key1, null, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        t2.delete(Long.MAX_VALUE);
        assertTransactionDeleted(t2);
    }
    
    @Test
    public void resumeAndCommitAfterTransientApplyFailure() {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> applyAndKeepLock(Request request, Map<String, AttributeValue> lockedItem) {
                throw new FailedYourRequestException();
            }  
        };
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item1));
            fail();
        } catch (FailedYourRequestException e) { } 
        
        assertItemLocked(getHashTableName(), key1, key1, t1.getId(), true, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        assertItemLocked(getHashTableName(), key2, t1.getId(), true, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        Transaction t3 = manager.resumeTransaction(t1.getId());
        t3.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
        assertItemNotLocked(getHashTableName(), key2, false);
        
        assertOldItemImage(t1.getId(), getHashTableName(), key1, null, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        t3.commit();
        
        t3.delete(Long.MAX_VALUE);
        assertTransactionDeleted(t2);
    }
    
    @Test
    public void applyOnlyOnce() {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> applyAndKeepLock(Request request, Map<String, AttributeValue> lockedItem) {
                super.applyAndKeepLock(request, lockedItem);
                throw new FailedYourRequestException();
            }  
        };
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue().withN("1"));
        
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("attr1", new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(new AttributeValue().withN("1")));
        
        UpdateItemRequest update = new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withAttributeUpdates(updates)
            .withKey(key1);
        
        try {
            t1.updateItem(update);
            fail();
        } catch (FailedYourRequestException e) { } 
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        assertItemLocked(getHashTableName(), key2, t1.getId(), true, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        t2.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
        assertItemNotLocked(getHashTableName(), key2, false);
        
        assertOldItemImage(t1.getId(), getHashTableName(), key1, null, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        t2.delete(Long.MAX_VALUE);
        assertTransactionDeleted(t2);
    }
    
    @Test
    public void resumeRollbackAfterTransientApplyFailure() {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> applyAndKeepLock(Request request, Map<String, AttributeValue> lockedItem) {
                throw new FailedYourRequestException();
            }  
        };
        
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item1));
            fail();
        } catch (FailedYourRequestException e) { } 
        
        assertItemLocked(getHashTableName(), key1, key1, t1.getId(), true, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        
        Transaction t2 = manager.resumeTransaction(t1.getId());
        
        t2.getItem(new GetItemRequest()
            .withTableName(getHashTableName())
            .withKey(key2));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        assertOldItemImage(t1.getId(), getHashTableName(), key1, key1, false);
        assertItemLocked(getHashTableName(), key2, t1.getId(), true, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        Transaction t3 = manager.resumeTransaction(t1.getId());
        t3.rollback();
        
        assertItemNotLocked(getHashTableName(), key1, false);
        assertItemNotLocked(getHashTableName(), key2, false);
        
        assertOldItemImage(t1.getId(), getHashTableName(), key1, null, false);
        assertOldItemImage(t1.getId(), getHashTableName(), key2, null, false);
        
        t3.delete(Long.MAX_VALUE);
        assertTransactionDeleted(t2);
    }
    
    @Test
    public void unlockInRollbackIfNoItemImageSaved() throws InterruptedException {
        final Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected void saveItemImage(Request callerRequest, Map<String, AttributeValue> item) {
                throw new FailedYourRequestException();
            }
        };
        
        // Change the existing item key0, failing when trying to save away the item image
        final Map<String, AttributeValue> item0a = new HashMap<String, AttributeValue> (item0);
        item0a.put("attr1", new AttributeValue("original1"));
        
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item0a));
            fail();
        } catch (FailedYourRequestException e) {}
        
        assertItemLocked(getHashTableName(), key0, item0, t1.getId(), false, false);
        
        // Roll back, and ensure the item was reverted correctly
        t1.rollback();
        
        assertItemNotLocked(getHashTableName(), key0, item0, true);
    }
    
    @Test
    public void shouldNotApplyAfterRollback() throws InterruptedException {
        final Semaphore barrier = new Semaphore(0);
        final Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> lockItem(Request callerRequest, boolean expectExists, int attempts)
                throws ItemNotLockedException, TransactionException {
                try {
                    barrier.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.lockItem(callerRequest, expectExists, attempts);
            }
        };
        
        final Map<String, AttributeValue> key1 = newKey(getHashTableName());
        final Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        final Semaphore caughtRolledBackException = new Semaphore(0);
        
        Thread thread = new Thread() {
            public void run() {
                try {
                    t1.putItem(new PutItemRequest()
                        .withTableName(getHashTableName())
                        .withItem(item1));
                } catch (TransactionRolledBackException e) {
                    caughtRolledBackException.release();
                }
            };
        };
        
        thread.start();
        
        assertItemNotLocked(getHashTableName(), key1, false);
        Transaction t2 = manager.resumeTransaction(t1.getId());
        t2.rollback();
        assertItemNotLocked(getHashTableName(), key1, false);
        
        barrier.release(100);
        
        thread.join();
  
        assertEquals(1, caughtRolledBackException.availablePermits());
        
        assertItemNotLocked(getHashTableName(), key1, false);
        assertTrue(t1.delete(Long.MIN_VALUE));
        
        // Now start a new transaction involving key1 and make sure it will complete
        final Map<String, AttributeValue> item1a = new HashMap<String, AttributeValue> (key1);
        item1a.put("attr1", new AttributeValue("new"));
        
        Transaction t3 = manager.newTransaction();
        t3.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1a));
        assertItemLocked(getHashTableName(), key1, item1a, t3.getId(), true, true);
        t3.commit();
        assertItemNotLocked(getHashTableName(), key1, item1a, true);
    }
    
    @Test
    public void shouldNotApplyAfterRollbackAndDeleted() throws InterruptedException {
        // Very similar to "shouldNotApplyAfterRollback" except the transaction is rolled back and then deleted.
        final Semaphore barrier = new Semaphore(0);
        final Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> lockItem(Request callerRequest, boolean expectExists, int attempts)
                throws ItemNotLockedException, TransactionException {
                try {
                    barrier.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.lockItem(callerRequest, expectExists, attempts);
            }
        };
        
        final Map<String, AttributeValue> key1 = newKey(getHashTableName());
        final Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        final Semaphore caughtTransactionNotFoundException = new Semaphore(0);
        
        Thread thread = new Thread() {
            public void run() {
                try {
                    t1.putItem(new PutItemRequest()
                        .withTableName(getHashTableName())
                        .withItem(item1));
                } catch (TransactionNotFoundException e) {
                    caughtTransactionNotFoundException.release();
                }
            };
        };
        
        thread.start();
        
        assertItemNotLocked(getHashTableName(), key1, false);
        Transaction t2 = manager.resumeTransaction(t1.getId());
        t2.rollback();
        assertItemNotLocked(getHashTableName(), key1, false);
        assertTrue(t2.delete(Long.MIN_VALUE)); // This is the key difference with shouldNotApplyAfterRollback
        
        barrier.release(100);
        
        thread.join();
  
        assertEquals(1, caughtTransactionNotFoundException.availablePermits());
        
        assertItemNotLocked(getHashTableName(), key1, false);
        
        // Now start a new transaction involving key1 and make sure it will complete
        final Map<String, AttributeValue> item1a = new HashMap<String, AttributeValue> (key1);
        item1a.put("attr1", new AttributeValue("new"));
        
        Transaction t3 = manager.newTransaction();
        t3.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1a));
        assertItemLocked(getHashTableName(), key1, item1a, t3.getId(), true, true);
        t3.commit();
        assertItemNotLocked(getHashTableName(), key1, item1a, true);
    }
    
    @Test
    public void shouldNotApplyAfterRollbackAndDeletedAndLeftLocked() throws InterruptedException {
        // Very similar to "shouldNotApplyAfterRollbackAndDeleted" except the lock is broken by a new transaction, not t1
        final Semaphore barrier = new Semaphore(0);
        final Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> lockItem(Request callerRequest, boolean expectExists, int attempts)
                throws ItemNotLockedException, TransactionException {
                try {
                    barrier.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.lockItem(callerRequest, expectExists, attempts);
            }
            
            @Override
            protected void releaseReadLock(String tableName, Map<String, AttributeValue> key) {
                throw new FailedYourRequestException();
            }
        };
        
        final Map<String, AttributeValue> key1 = newKey(getHashTableName());
        final Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue> (key1);
        item1.put("attr1", new AttributeValue("original1"));
        
        final Semaphore caughtFailedYourRequestException = new Semaphore(0);
        
        Thread thread = new Thread() {
            public void run() {
                try {
                    t1.putItem(new PutItemRequest()
                        .withTableName(getHashTableName())
                        .withItem(item1));
                } catch (FailedYourRequestException e) {
                    caughtFailedYourRequestException.release();
                }
            };
        };
        
        thread.start();
        
        assertItemNotLocked(getHashTableName(), key1, false);
        Transaction t2 = manager.resumeTransaction(t1.getId());
        t2.rollback();
        assertItemNotLocked(getHashTableName(), key1, false);
        assertTrue(t2.delete(Long.MIN_VALUE));
        
        barrier.release(100);
        
        thread.join();
  
        assertEquals(1, caughtFailedYourRequestException.availablePermits());
        
        assertItemLocked(getHashTableName(), key1, null, t1.getId(), true, false, false); // locked and "null", but don't check the tx item
        
        // Now start a new transaction involving key1 and make sure it will complete
        final Map<String, AttributeValue> item1a = new HashMap<String, AttributeValue> (key1);
        item1a.put("attr1", new AttributeValue("new"));
        
        Transaction t3 = manager.newTransaction();
        t3.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1a));
        assertItemLocked(getHashTableName(), key1, item1a, t3.getId(), true, true);
        t3.commit();
        assertItemNotLocked(getHashTableName(), key1, item1a, true);
    }

    // TODO same as shouldNotLockAndApplyAfterRollbackAndDeleted except make t3 do the unlock, not t1.
    
    @Test
    public void basicNewItemRollback() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        
        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1));
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, true);
        
        t1.rollback();
        assertItemNotLocked(getHashTableName(), key1, false);
        
        t1.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void basicNewItemCommit() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        
        t1.updateItem(new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(key1));
        assertItemLocked(getHashTableName(), key1, t1.getId(), true, true);
        
        t1.commit();
        assertItemNotLocked(getHashTableName(), key1, key1, true);
        t1.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void missingTableName() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        
        try {
            t1.updateItem(new UpdateItemRequest()
                .withKey(key1));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("TableName must not be null"));
        }
        assertItemNotLocked(getHashTableName(), key1, false);
        t1.rollback();
        assertItemNotLocked(getHashTableName(), key1, false);
        t1.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void emptyTransaction() {
        Transaction t1 = manager.newTransaction();
        t1.commit();
        t1.delete(Long.MAX_VALUE);
        assertTransactionDeleted(t1);
    }
    
    @Test
    public void missingKey() {
        Transaction t1 = manager.newTransaction();
        try {
            t1.updateItem(new UpdateItemRequest()
                .withTableName(getHashTableName()));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("The request key cannot be empty"));
        }
        t1.rollback();
        t1.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void tooMuchDataInTransaction() throws Exception {
        Transaction t1 = manager.newTransaction();
        Transaction t2 = manager.newTransaction();
        Map<String, AttributeValue> key1 = newKey(getHashTableName());
        Map<String, AttributeValue> key2 = newKey(getHashTableName());
        
        // Write item 1 as a starting point
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < 1024 * 40; i++) {
            sb.append("a");
        }
        String bigString = sb.toString();
        
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>(key1);
        item1.put("bigattr", new AttributeValue("little"));
        t1.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1));
        
        assertItemLocked(getHashTableName(), key1, item1, t1.getId(), true, true);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1, true);
        
        Map<String, AttributeValue> item1a = new HashMap<String, AttributeValue>(key1);
        item1a.put("bigattr", new AttributeValue(bigString));
        
        t2.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item1a));
        
        assertItemLocked(getHashTableName(), key1, item1a, t2.getId(), false, true);
        
        Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue>(key2);
        item2.put("bigattr", new AttributeValue(bigString));
        
        try {
            t2.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item2));
            fail();
        } catch (InvalidRequestException e) { }
        
        assertItemNotLocked(getHashTableName(), key2, false);
        assertItemLocked(getHashTableName(), key1, item1a, t2.getId(), false, true);
        
        item2.put("bigattr", new AttributeValue("fitsThisTime"));
        t2.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(item2));
        
        assertItemLocked(getHashTableName(), key2, item2, t2.getId(), true, true);
        
        t2.commit();
        
        assertItemNotLocked(getHashTableName(), key1, item1a, true);
        assertItemNotLocked(getHashTableName(), key2, item2, true);
        
        t1.delete(Long.MAX_VALUE);
        t2.delete(Long.MAX_VALUE);
    }
    
    @Test
    public void containsBinaryAttributes() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key = newKey(getHashTableName());
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(key);
        
        item.put("attr_b", new AttributeValue().withB(ByteBuffer.wrap(new String("asdf\n\t\u0123").getBytes())));
        item.put("attr_bs", new AttributeValue().withBS(
                ByteBuffer.wrap(new String("asdf\n\t\u0123").getBytes()), 
                ByteBuffer.wrap(new String("wef").getBytes())));
        
        t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item));
        
        assertItemLocked(getHashTableName(), key, item, t1.getId(), true, true);
        
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), key, item, true);
    }
    
    @Test
    public void containsSpecialAttributes() {
        Transaction t1 = manager.newTransaction();
        Map<String, AttributeValue> key = newKey(getHashTableName());
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(key);
        item.put(AttributeName.TXID.toString(), new AttributeValue("asdf"));
        
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().contains("must not contain the reserved"));
        }
        
        item.put(AttributeName.TRANSIENT.toString(), new AttributeValue("asdf"));
        item.remove(Transaction.AttributeName.TXID.toString());
        
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().contains("must not contain the reserved"));
        }
        
        item.put(AttributeName.APPLIED.toString(), new AttributeValue("asdf"));
        item.remove(AttributeName.TRANSIENT.toString());
        
        try {
            t1.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(item));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().contains("must not contain the reserved"));
        }
    }
    
    @Test
    public void itemTooLargeToLock() {
        
    }
    
    @Test
    public void itemTooLargeToApply() {
        
    }
    
    @Test
    public void itemTooLargeToSavePreviousVersion() {
        
    }
    
    @Test
    public void failover() throws Exception {
        Transaction t1 = new Transaction(UUID.randomUUID().toString(), manager, true) {
            @Override
            protected Map<String, AttributeValue> lockItem(Request callerRequest, boolean expectExists, int attempts)
                throws ItemNotLockedException, TransactionException {
                
                throw new FailedYourRequestException();
            }
        };
        
        // prepare a request
        UpdateItemRequest callerRequest = new UpdateItemRequest()
            .withTableName(getHashTableName())
            .withKey(newKey(getHashTableName()));
        
        try {
            t1.updateItem(callerRequest);
            fail();
        } catch (FailedYourRequestException e) { }
        assertItemNotLocked(getHashTableName(), callerRequest.getKey(), false);
        
        // The non-failing manager
        Transaction t2 = manager.resumeTransaction(t1.getId());
        t2.commit();
        
        assertItemNotLocked(getHashTableName(), callerRequest.getKey(), true);
        
        // If this attempted to apply again, this would fail because of the failing client
        t1.commit();
        
        assertItemNotLocked(getHashTableName(), callerRequest.getKey(), true);
        
        t1.delete(Long.MAX_VALUE);
        t2.delete(Long.MAX_VALUE);
    }
    
    private void assertItemLocked(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> expected, String owner, boolean isTransient, boolean isApplied) {
        assertItemLocked(tableName, key, expected, owner, isTransient, isApplied, true);
    }
    
    private void assertItemLocked(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> expected, String owner, boolean isTransient, boolean isApplied, boolean checkTxItem) {
        Map<String, AttributeValue> item = getItem(tableName, key);
        assertNotNull(item);
        assertEquals(owner, item.get(AttributeName.TXID.toString()).getS());
        if(isTransient) {
            assertTrue("item is not transient, and should have been", item.containsKey(AttributeName.TRANSIENT.toString()));
            assertEquals("item is not transient, and should have been", "1", item.get(AttributeName.TRANSIENT.toString()).getS());    
        } else {
            assertNull("item is transient, and should not have been", item.get(AttributeName.TRANSIENT.toString()));
        }
        if(isApplied) {
            assertTrue("item is not applied, and should have been", item.containsKey(AttributeName.APPLIED.toString()));
            assertEquals("item is not applied, and should have been", "1", item.get(AttributeName.APPLIED.toString()).getS());
        } else {
            assertNull("item is applied, and should not have been", item.get(AttributeName.APPLIED.toString()));
        }
        assertTrue(item.containsKey(AttributeName.DATE.toString()));
        if(expected != null) {
            item.remove(AttributeName.TXID.toString());
            item.remove(AttributeName.TRANSIENT.toString());
            item.remove(AttributeName.APPLIED.toString());
            item.remove(AttributeName.DATE.toString());
            assertEquals(expected, item);
        }
        // Also verify that it is locked in the tx record
        if(checkTxItem) {
            TransactionItem txItem = new TransactionItem(owner, manager, false);
            assertTrue(txItem.getRequestMap().containsKey(tableName));
            assertTrue(txItem.getRequestMap().get(tableName).containsKey(new ImmutableKey(key)));
        }
    }
    
    private void assertItemLocked(String tableName, Map<String, AttributeValue> key, String owner, boolean isTransient, boolean isApplied) {
        assertItemLocked(tableName, key, null, owner, isTransient, isApplied);
    }
    
    private void assertItemNotLocked(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> expected, boolean shouldExist) {
        Map<String, AttributeValue> item = getItem(tableName, key);
        if(shouldExist) {
            assertNotNull("Item does not exist in the table, but it should", item);
            assertNull(item.get(AttributeName.TRANSIENT.toString()));
            assertNull(item.get(AttributeName.TXID.toString()));
            assertNull(item.get(AttributeName.APPLIED.toString()));
            assertNull(item.get(AttributeName.DATE.toString()));
        } else {
            assertNull("Item should have been null: " + item, item);
        }
        
        if(expected != null) {
            item.remove(AttributeName.TXID.toString());
            item.remove(AttributeName.TRANSIENT.toString());
            assertEquals(expected, item);
        }
    }
    
    private void assertItemNotLocked(String tableName, Map<String, AttributeValue> key, boolean shouldExist) {
        assertItemNotLocked(tableName, key, null, shouldExist);
    }
    
    protected void assertTransactionDeleted(Transaction t) {
        try {
            manager.resumeTransaction(t.getId());
            fail();
        } catch (TransactionNotFoundException e) { }
    }
    
    protected void assertNoSpecialAttributes(Map<String, AttributeValue> item) {
        for(String attrName : Transaction.SPECIAL_ATTR_NAMES) {
            if(item.containsKey(attrName)) {
                fail("Should not have contained attribute " + attrName + " " + item);
            }
        }
    }
    
    protected void assertOldItemImage(String txId, String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> item, boolean shouldExist) {
        Transaction t = manager.resumeTransaction(txId);
        Map<String, HashMap<ImmutableKey, Request>> requests = t.getTxItem().getRequestMap();
        Request r = requests.get(tableName).get(new ImmutableKey(key));
        Map<String, AttributeValue> image = t.getTxItem().loadItemImage(r.getRid());
        if(shouldExist) {
            assertNotNull(image);
            image.remove(AttributeName.TXID.toString());
            image.remove(AttributeName.IMAGE_ID.toString());
            image.remove(AttributeName.DATE.toString());
            assertTrue(! image.containsKey(AttributeName.TRANSIENT.toString()));
            assertEquals(item, image);
        } else { 
            assertNull(image);
        }
    }
    
    private Map<String, AttributeValue> getItem(String tableName, Map<String, AttributeValue> key) {
        GetItemResult result = dynamodb.getItem(new GetItemRequest()
            .withTableName(tableName)
            .withKey(key)
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .withConsistentRead(true));
        return result.getItem();
    }
    
    @Test
    public void oneTransactionPerItem() {
        Transaction transaction = manager.newTransaction();
        Map<String, AttributeValue> key = newKey(getHashTableName());
        
        transaction.putItem(new PutItemRequest()
            .withTableName(getHashTableName())
            .withItem(key));
        try {
            transaction.putItem(new PutItemRequest()
                .withTableName(getHashTableName())
                .withItem(key));
            fail();
        } catch(DuplicateRequestException e) {
            transaction.rollback();
        }
        assertItemNotLocked(getHashTableName(), key, false);
        transaction.delete(Long.MAX_VALUE);
    }
}
 