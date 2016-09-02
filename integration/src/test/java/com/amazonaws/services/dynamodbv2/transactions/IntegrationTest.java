/**
 * Copyright 2015-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionNotFoundException;
import com.amazonaws.services.dynamodbv2.util.ImmutableKey;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class IntegrationTest {
    
    protected static final FailingAmazonDynamoDBClient dynamodb;
    protected static final DynamoDB documentDynamoDB;
    private static final String DYNAMODB_ENDPOINT = "http://dynamodb.us-west-2.amazonaws.com";
    private static final String DYNAMODB_ENDPOINT_PROPERTY = "dynamodb-local.endpoint";

    protected static final String ID_ATTRIBUTE = "Id";
    protected static final String HASH_TABLE_NAME = "TransactionsIntegrationTest_Hash";
    protected static final String HASH_RANGE_TABLE_NAME = "TransactionsIntegrationTest_HashRange";
    protected static final String LOCK_TABLE_NAME = "TransactionsIntegrationTest_Transactions";
    protected static final String IMAGES_TABLE_NAME = "TransactionsIntegrationTest_ItemImages";
    protected static final String TABLE_NAME_PREFIX = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    protected static final String INTEG_LOCK_TABLE_NAME = TABLE_NAME_PREFIX + "_" + LOCK_TABLE_NAME;
    protected static final String INTEG_IMAGES_TABLE_NAME = TABLE_NAME_PREFIX + "_" + IMAGES_TABLE_NAME;
    protected static final String INTEG_HASH_TABLE_NAME = TABLE_NAME_PREFIX + "_" + HASH_TABLE_NAME;
    protected static final String INTEG_HASH_RANGE_TABLE_NAME = TABLE_NAME_PREFIX + "_" + HASH_RANGE_TABLE_NAME;

    protected final TransactionManager manager;

    public IntegrationTest() {
        manager = new TransactionManager(dynamodb, INTEG_LOCK_TABLE_NAME, INTEG_IMAGES_TABLE_NAME);
    }

    public IntegrationTest(DynamoDBMapperConfig config) {
        manager = new TransactionManager(dynamodb, INTEG_LOCK_TABLE_NAME, INTEG_IMAGES_TABLE_NAME, config);
    }

    static {
        AWSCredentials credentials;
        String endpoint = System.getProperty(DYNAMODB_ENDPOINT_PROPERTY);
        if (endpoint != null) {
            credentials = new BasicAWSCredentials("local", "local");
        } else {
            endpoint = DYNAMODB_ENDPOINT;
            try {
                credentials = new PropertiesCredentials(
                    TransactionsIntegrationTest.class.getResourceAsStream("AwsCredentials.properties"));
                if(credentials.getAWSAccessKeyId().isEmpty()) {
                    System.err.println("No credentials supplied in AwsCredentials.properties, will try with default credentials file");
                    credentials = new ProfileCredentialsProvider().getCredentials();
                }
            } catch (IOException e) {
                System.err.println("Could not load credentials from built-in credentials file.");
                throw new RuntimeException(e);
            }
        }

        dynamodb = new FailingAmazonDynamoDBClient(credentials);
        dynamodb.setEndpoint(endpoint);

        documentDynamoDB = new DynamoDB(dynamodb);
    }

    protected Map<String, AttributeValue> key0;
    protected Map<String, AttributeValue> item0;

    protected Map<String, AttributeValue> newKey(String tableName) {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put(ID_ATTRIBUTE, new AttributeValue().withS("val_" + Math.random()));
        if (INTEG_HASH_RANGE_TABLE_NAME.equals(tableName)) {
            key.put("RangeAttr", new AttributeValue().withN(Double.toString(Math.random())));
        } else if(!INTEG_HASH_TABLE_NAME.equals(tableName)){
            throw new IllegalArgumentException();
        }
        return key;
    }

    private static void waitForTableToBecomeAvailable(String tableName) {
        Table tableToWaitFor = documentDynamoDB.getTable(tableName);
        try {
            System.out.println("Waiting for " + tableName + " to become ACTIVE...");
            tableToWaitFor.waitForActive();
        } catch (Exception e) {
            throw new RuntimeException("Table " + tableName + " never went active");
        }
    }

    @BeforeClass
    public static void createTables() throws InterruptedException {
        try {
            CreateTableRequest createHash = new CreateTableRequest()
                .withTableName(INTEG_HASH_TABLE_NAME)
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName(ID_ATTRIBUTE).withAttributeType(ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement().withAttributeName(ID_ATTRIBUTE).withKeyType(KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
            dynamodb.createTable(createHash);
        } catch (ResourceInUseException e) {
            System.err.println("Warning: " + INTEG_HASH_TABLE_NAME + " was already in use");
        }

        try {
            TransactionManager.verifyOrCreateTransactionTable(dynamodb, INTEG_LOCK_TABLE_NAME, 10L, 10L, 5L * 60);
            TransactionManager.verifyOrCreateTransactionImagesTable(dynamodb, INTEG_IMAGES_TABLE_NAME, 10L, 10L, 5L * 60);
        } catch (ResourceInUseException e) {
            System.err.println("Warning: " + INTEG_HASH_TABLE_NAME + " was already in use");
        }

        waitForTableToBecomeAvailable(INTEG_HASH_TABLE_NAME);
        waitForTableToBecomeAvailable(INTEG_LOCK_TABLE_NAME);
        waitForTableToBecomeAvailable(INTEG_IMAGES_TABLE_NAME);
    }

    @AfterClass
    public static void deleteTables() throws InterruptedException {
        try {
            Table hashTable = documentDynamoDB.getTable(INTEG_HASH_TABLE_NAME);
            Table lockTable = documentDynamoDB.getTable(INTEG_LOCK_TABLE_NAME);
            Table imagesTable = documentDynamoDB.getTable(INTEG_IMAGES_TABLE_NAME);

            System.out.println("Issuing DeleteTable request for " + INTEG_HASH_TABLE_NAME);
            hashTable.delete();
            System.out.println("Issuing DeleteTable request for " + INTEG_LOCK_TABLE_NAME);
            lockTable.delete();
            System.out.println("Issuing DeleteTable request for " + INTEG_IMAGES_TABLE_NAME);
            imagesTable.delete();

            System.out.println("Waiting for " + INTEG_HASH_TABLE_NAME + " to be deleted...this may take a while...");
            hashTable.waitForDelete();
            System.out.println("Waiting for " + INTEG_LOCK_TABLE_NAME + " to be deleted...this may take a while...");
            lockTable.waitForDelete();
            System.out.println("Waiting for " + INTEG_IMAGES_TABLE_NAME + " to be deleted...this may take a while...");
            imagesTable.waitForDelete();
        } catch (Exception e) {
            System.err.println("DeleteTable request failed for some table");
            System.err.println(e.getMessage());
        }
    }

    protected void assertItemLocked(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> expected, String owner, boolean isTransient, boolean isApplied) {
        assertItemLocked(tableName, key, expected, owner, isTransient, isApplied, true /*checkTxItem*/);
    }

    protected void assertItemLocked(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> expected, String owner, boolean isTransient, boolean isApplied, boolean checkTxItem) {
        Map<String, AttributeValue> item = getItem(tableName, key);
        assertNotNull(item);
        assertEquals(owner, item.get(Transaction.AttributeName.TXID.toString()).getS());
        if(isTransient) {
            assertTrue("item is not transient, and should have been", item.containsKey(Transaction.AttributeName.TRANSIENT.toString()));
            assertEquals("item is not transient, and should have been", "1", item.get(Transaction.AttributeName.TRANSIENT.toString()).getS());
        } else {
            assertNull("item is transient, and should not have been", item.get(Transaction.AttributeName.TRANSIENT.toString()));
        }
        if(isApplied) {
            assertTrue("item is not applied, and should have been", item.containsKey(Transaction.AttributeName.APPLIED.toString()));
            assertEquals("item is not applied, and should have been", "1", item.get(Transaction.AttributeName.APPLIED.toString()).getS());
        } else {
            assertNull("item is applied, and should not have been", item.get(Transaction.AttributeName.APPLIED.toString()));
        }
        assertTrue(item.containsKey(Transaction.AttributeName.DATE.toString()));
        if(expected != null) {
            item.remove(Transaction.AttributeName.TXID.toString());
            item.remove(Transaction.AttributeName.TRANSIENT.toString());
            item.remove(Transaction.AttributeName.APPLIED.toString());
            item.remove(Transaction.AttributeName.DATE.toString());
            assertEquals(expected, item);
        }
        // Also verify that it is locked in the tx record
        if(checkTxItem) {
            TransactionItem txItem = new TransactionItem(owner, manager, false /*insert*/);
            assertTrue(txItem.getRequestMap().containsKey(tableName));
            assertTrue(txItem.getRequestMap().get(tableName).containsKey(new ImmutableKey(key)));
        }
    }

    protected void assertItemLocked(String tableName, Map<String, AttributeValue> key, String owner, boolean isTransient, boolean isApplied) {
        assertItemLocked(tableName, key, null /*expected*/, owner, isTransient, isApplied);
    }

    protected void assertItemNotLocked(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValue> expected, boolean shouldExist) {
        Map<String, AttributeValue> item = getItem(tableName, key);
        if(shouldExist) {
            assertNotNull("Item does not exist in the table, but it should", item);
            assertNull(item.get(Transaction.AttributeName.TRANSIENT.toString()));
            assertNull(item.get(Transaction.AttributeName.TXID.toString()));
            assertNull(item.get(Transaction.AttributeName.APPLIED.toString()));
            assertNull(item.get(Transaction.AttributeName.DATE.toString()));
        } else {
            assertNull("Item should have been null: " + item, item);
        }

        if(expected != null) {
            item.remove(Transaction.AttributeName.TXID.toString());
            item.remove(Transaction.AttributeName.TRANSIENT.toString());
            assertEquals(expected, item);
        }
    }

    protected void assertItemNotLocked(String tableName, Map<String, AttributeValue> key, boolean shouldExist) {
        assertItemNotLocked(tableName, key, null, shouldExist);
    }

    protected void assertTransactionDeleted(Transaction t) {
        try {
            manager.resumeTransaction(t.getId());
            fail();
        } catch (TransactionNotFoundException e) {
            assertTrue(e.getMessage().contains("Transaction not found"));
        }
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
            image.remove(Transaction.AttributeName.TXID.toString());
            image.remove(Transaction.AttributeName.IMAGE_ID.toString());
            image.remove(Transaction.AttributeName.DATE.toString());
            assertFalse(image.containsKey(Transaction.AttributeName.TRANSIENT.toString()));
            assertEquals(item, image);  // TODO does not work for Set AttributeValue types (DynamoDB does not preserve ordering)
        } else {
            assertNull(image);
        }
    }

    protected Map<String, AttributeValue> getItem(String tableName, Map<String, AttributeValue> key) {
        GetItemResult result = dynamodb.getItem(new GetItemRequest()
            .withTableName(tableName)
            .withKey(key)
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .withConsistentRead(true));
        return result.getItem();
    }

}