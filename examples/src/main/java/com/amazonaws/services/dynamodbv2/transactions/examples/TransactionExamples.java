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
 package com.amazonaws.services.dynamodbv2.transactions.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.transactions.Transaction;
import com.amazonaws.services.dynamodbv2.transactions.TransactionManager;
import com.amazonaws.services.dynamodbv2.transactions.Transaction.IsolationLevel;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.DuplicateRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.InvalidRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.ItemNotLockedException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionRolledBackException;
import com.amazonaws.services.dynamodbv2.util.TableHelper;

/**
 * Demonstrates creating the required transactions tables, an example user table, and performs several transactions
 * to demonstrate the best practices for using this library.
 * 
 * To use this library, you will need to fill in the AwsCredentials.properties file with credentials for your account, 
 * or otherwise modify this file to inject your credentials in another way (such as by using IAM Roles for EC2) 
 */
public class TransactionExamples {

    protected static final String TX_TABLE_NAME = "Transactions";
    protected static final String TX_IMAGES_TABLE_NAME = "TransactionImages";
    protected static final String EXAMPLE_TABLE_NAME = "TransactionExamples";
    protected static final String EXAMPLE_TABLE_HASH_KEY = "ItemId";
    protected static final String DYNAMODB_ENDPOINT = "http://dynamodb.us-west-2.amazonaws.com";
    
    protected final AmazonDynamoDBClient dynamodb;
    protected final TransactionManager txManager;
    
    public static void main(String[] args) {
        print("Running DynamoDB transaction examples");
        try {
            new TransactionExamples().run();
            print("Exiting normally");
        } catch (Throwable t) {
            System.err.println("Uncaught exception:" + t);
            t.printStackTrace(System.err);
        }
    }
    
    public TransactionExamples() {
        AWSCredentials credentials;

        try {
            credentials = new PropertiesCredentials(
                TransactionExamples.class.getResourceAsStream("AwsCredentials.properties"));
            if(credentials.getAWSAccessKeyId().isEmpty()) {
                System.err.println("No credentials supplied in AwsCredentials.properties, will try with default credentials file");
                credentials = new ProfileCredentialsProvider().getCredentials();
            }
        } catch (IOException e) {
            System.err.println("Could not load credentials from built-in credentials file.");
            throw new RuntimeException(e);
        }

        dynamodb = new AmazonDynamoDBClient(credentials);
        dynamodb.setEndpoint(DYNAMODB_ENDPOINT);
        txManager = new TransactionManager(dynamodb, TX_TABLE_NAME, TX_IMAGES_TABLE_NAME);
    }
    
    public void run() throws Exception {
        setup();
        twoItemTransaction();
        conflictingTransactions();
        errorHandling();
        badRequest();
        readThenWrite();
        conditionallyCreateOrUpdateWithMapper();
        reading();
        readCommittedWithMapper();
        sweepForStuckAndOldTransactions();
    }
    
    public void setup() throws Exception {
        print("\n*** setup() ***\n");
        TableHelper tableHelper = new TableHelper(dynamodb);
        
        // 1. Verify that the transaction table exists, or create it if it doesn't exist
        print("Verifying or creating table " + TX_TABLE_NAME);
        TransactionManager.verifyOrCreateTransactionTable(dynamodb, TX_TABLE_NAME, 1, 1, null);
        
        // 2. Verify that the transaction item images table exists, or create it otherwise
        print("Verifying or creating table " + TX_IMAGES_TABLE_NAME);
        TransactionManager.verifyOrCreateTransactionImagesTable(dynamodb, TX_IMAGES_TABLE_NAME, 1, 1, null);
        
        // 3. Create a table to do transactions on
        print("Verifying or creating table " + EXAMPLE_TABLE_NAME);
        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
            new AttributeDefinition().withAttributeName(EXAMPLE_TABLE_HASH_KEY).withAttributeType(ScalarAttributeType.S));
        List<KeySchemaElement> keySchema = Arrays.asList(
            new KeySchemaElement().withAttributeName(EXAMPLE_TABLE_HASH_KEY).withKeyType(KeyType.HASH));
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L);
        
        tableHelper.verifyOrCreateTable(EXAMPLE_TABLE_NAME, attributeDefinitions, keySchema, null, provisionedThroughput, null);
        
        // 4. Wait for tables to be created
        print("Waiting for table to become ACTIVE: " + EXAMPLE_TABLE_NAME);
        tableHelper.waitForTableActive(EXAMPLE_TABLE_NAME, 5 * 60L);
        print("Waiting for table to become ACTIVE: " + TX_TABLE_NAME);
        tableHelper.waitForTableActive(TX_TABLE_NAME, 5 * 60L);
        print("Waiting for table to become ACTIVE: " + TX_IMAGES_TABLE_NAME);
        tableHelper.waitForTableActive(TX_IMAGES_TABLE_NAME, 5 * 60L);
    }
    
    /**
     * This example writes two items.
     */
    public void twoItemTransaction() {
        print("\n*** twoItemTransaction() ***\n");
        
        // Create a new transaction from the transaction manager
        Transaction t1 = txManager.newTransaction();
        
        // Add a new PutItem request to the transaction object (instead of on the AmazonDynamoDB client)
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
        item1.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("Item1"));
        print("Put item: " + item1);
        t1.putItem(new PutItemRequest()
            .withTableName(EXAMPLE_TABLE_NAME)
            .withItem(item1));
        print("At this point Item1 is in the table, but is not yet committed");
        
        // Add second PutItem request for a different item to the transaction object
        Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue>();
        item2.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("Item2"));
        print("Put item: " + item2);
        t1.putItem(new PutItemRequest()
            .withTableName(EXAMPLE_TABLE_NAME)
            .withItem(item2));
        print("At this point Item2 is in the table, but is not yet committed");
        
        // Commit the transaction.  
        t1.commit();
        print("Committed transaction.  Item1 and Item2 are now both committed.");
        
        t1.delete();
        print("Deleted the transaction item.");
    }
    
    /**
     * This example demonstrates two transactions attempting to write to the same item.  
     * Only one transaction will go through.
     */
    public void conflictingTransactions() {
        print("\n*** conflictingTransactions() ***\n");
        // Start transaction t1
        Transaction t1 = txManager.newTransaction();
        
        // Construct a primary key of an item that will overlap between two transactions.
        Map<String, AttributeValue> item1Key = new HashMap<String, AttributeValue>();
        item1Key.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("conflictingTransactions_Item1"));
        item1Key = Collections.unmodifiableMap(item1Key);
        
        // Add a new PutItem request to the transaction object (instead of on the AmazonDynamoDB client)
        // This will eventually get rolled back when t2 tries to work on the same item
        Map<String, AttributeValue> item1T1 = new HashMap<String, AttributeValue>(item1Key);
        item1T1.put("WhichTransaction?", new AttributeValue("t1"));
        print("T1 - Put item: " + item1T1);
        t1.putItem(new PutItemRequest()
            .withTableName(EXAMPLE_TABLE_NAME)
            .withItem(item1T1));
        print("T1 - At this point Item1 is in the table, but is not yet committed");
        
        Map<String, AttributeValue> item2T1 = new HashMap<String, AttributeValue>();
        item2T1.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("conflictingTransactions_Item2"));
        print("T1 - Put a second, non-overlapping item: " + item2T1);
        t1.putItem(new PutItemRequest()
            .withTableName(EXAMPLE_TABLE_NAME)
            .withItem(item2T1));
        print("T1 - At this point Item2 is also in the table, but is not yet committed");
        
        // Start a new transaction t2
        Transaction t2 = txManager.newTransaction();
        
        Map<String, AttributeValue> item1T2 = new HashMap<String, AttributeValue>(item1Key);
        item1T1.put("WhichTransaction?", new AttributeValue("t2 - I win!"));
        print("T2 - Put item: " + item1T2);
        t2.putItem(new PutItemRequest()
            .withTableName(EXAMPLE_TABLE_NAME)
            .withItem(item1T2));
        print("T2 - At this point Item1 from t2 is in the table, but is not yet committed. t1 was rolled back.");
        
        // To prove that t1 will have been rolled back by this point, attempt to commit it.
        try {
            print("Attempting to commit t1 (this will fail)");
            t1.commit();
            throw new RuntimeException("T1 should have been rolled back. This is a bug.");
        } catch(TransactionRolledBackException e) {
            print("Transaction t1 was rolled back, as expected");
            t1.delete(); // Delete it, no longer needed
        }
        
        // Now put a second item as a part of t2
        Map<String, AttributeValue> item3T2 = new HashMap<String, AttributeValue>();
        item3T2.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("conflictingTransactions_Item3"));
        print("T2 - Put item: " + item3T2);
        t2.putItem(new PutItemRequest()
            .withTableName(EXAMPLE_TABLE_NAME)
            .withItem(item3T2));
        print("T2 - At this point Item3 is in the table, but is not yet committed");
        
        print("Committing and deleting t2");
        t2.commit();
        
        t2.delete();
        
        // Now to verify, get the items Item1, Item2, and Item3.
        // More on read operations later. 
        GetItemResult result = txManager.getItem(new GetItemRequest().withTableName(EXAMPLE_TABLE_NAME).withKey(item1Key), IsolationLevel.UNCOMMITTED);
        print("Notice that t2's write to Item1 won: " + result.getItem());
        
        result = txManager.getItem(new GetItemRequest().withTableName(EXAMPLE_TABLE_NAME).withKey(item3T2), IsolationLevel.UNCOMMITTED);
        print("Notice that t2's write to Item3 also went through: " + result.getItem());
        
        result = txManager.getItem(new GetItemRequest().withTableName(EXAMPLE_TABLE_NAME).withKey(item2T1), IsolationLevel.UNCOMMITTED);
        print("However, t1's write to Item2 did not go through (since Item2 is null): " + result.getItem());
    }
   
    /**
     * This example shows the kinds of exceptions that you might need to handle
     */
    public void errorHandling() {
        print("\n*** errorHandling() ***\n");
        
        // Create a new transaction from the transaction manager
        Transaction t1 = txManager.newTransaction();
        
        boolean success = false;
        try {
            // Add a new PutItem request to the transaction object (instead of on the AmazonDynamoDB client)
            Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
            item1.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("Item1"));
            print("Put item: " + item1);
            t1.putItem(new PutItemRequest()
                .withTableName(EXAMPLE_TABLE_NAME)
                .withItem(item1));
            
            // Commit the transaction.  
            t1.commit();
            success = true;
            print("Committed transaction.  We aren't actually expecting failures in this example.");
        } catch (TransactionRolledBackException e) {
            // This gets thrown if the transaction was rolled back by another transaction
            throw e;
        } catch (ItemNotLockedException e) {
            // This gets thrown if there is too much contention with other transactions for the item you're trying to lock
            throw e;
        } catch (DuplicateRequestException e) {
            // This happens if you try to do two write operations on the same item in the same transaction
            throw e;
        } catch (InvalidRequestException e) {
            // This happens if you do something like forget the TableName or key attributes in the request
            throw e;
        } catch (TransactionException e) {
            // All exceptions thrown directly by this library derive from this.  It is a catch-all
            throw e;
        } catch (AmazonServiceException e) {
            // However, your own requests can still fail if they're invalid.  For example, you can get a 
            // ValidationException if you try to add a "number" to a "string" in UpdateItem.  So you have to handle
            // errors from DynamoDB in the same way you did before.  Except now you should roll back the transaction if it fails.
            throw e;
        } finally {
            if(! success) {
                // It can be a good idea to use a "success" flag in this way, to ensure that you roll back if you get any exceptions 
                // from the transaction library, or from DynamoDB, or any from the DynamoDB client library.  These 3 exception base classes are:
                // TransactionException, AmazonServiceException, or AmazonClientExeption.
                // If you forget to roll back, no problem - another transaction will come along and roll yours back eventually.
                try {
                    t1.rollback();
                } catch (TransactionException te) { } // ignore, but should probably log
            }
            
            try {
                t1.delete();
            } catch (TransactionException te) { } // ignore, but should probably log
        }
    }
    
    /**
     * This example shows an example of how to handle errors
     */
    public void badRequest() throws RuntimeException {
        print("\n*** badRequest() ***\n");
        
        // Create a "success" flag and set it to false.  We'll roll back the transaction in a finally {} if this wasn't set to true by then.
        boolean success = false;
        Transaction t1 = txManager.newTransaction();
        
        try {
            // Construct a request that we know DynamoDB will reject.
            Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
            key.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("Item1"));
            
            // You cannot "add" a String type attribute.  This request will be rejected by DynamoDB.
            Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
            updates.put("Will this work?", new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(new AttributeValue("Nope.")));
            
            // The transaction library will make the request here, so we actually see
            print("Making invalid request");
            t1.updateItem(new UpdateItemRequest()
                .withTableName(EXAMPLE_TABLE_NAME)
                .withKey(key)
                .withAttributeUpdates(updates));
            
            t1.commit();
            success = true;
            throw new RuntimeException("This should NOT have happened (actually should have failed before commit)");
        } catch (AmazonServiceException e) {
            print("Caught a ValidationException. This is what we expected. The transaction will be rolled back: " + e.getMessage());
            // in a real application, you'll probably want to throw an exception to your caller 
        } finally {
            if(! success) {
                print("The transaction didn't work, as expected.  Rolling back.");
                // It can be a good idea to use a "success" flag in this way, to ensure that you roll back if you get any exceptions 
                // from the transaction library, or from DynamoDB, or any from the DynamoDB client library.  These 3 exception base classes are:
                // TransactionException, AmazonServiceException, or AmazonClientExeption.
                // If you forget to roll back, no problem - another transaction will come along and roll yours back eventually.
                try {
                    t1.rollback();
                } catch (TransactionException te) { } // ignore, but should probably log
            }
            
            try {
                t1.delete();
            } catch (TransactionException te) { } // ignore, but should probably log
        }
    }
    
    /**
     * This example shows that reads can be performed in a transaction, and read locks can be upgraded to write locks. 
     */
    public void readThenWrite() {
        print("\n*** readThenWrite() ***\n");
        
        Transaction t1 = txManager.newTransaction();
        
        // Perform a GetItem request on the transaction
        print("Reading Item1");
        Map<String, AttributeValue> key1 = new HashMap<String, AttributeValue>();
        key1.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("Item1"));
        
        Map<String, AttributeValue> item1 = t1.getItem(new GetItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)).getItem();
        print("Item1: " + item1);
        
        // Now call UpdateItem to add a new attribute.
        // Notice that the library supports ReturnValues in writes
        print("Updating Item1");
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("Color", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("Green")));
        
        item1 = t1.updateItem(new UpdateItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        print("Item1 is now: " + item1);
        
        t1.commit();
        
        t1.delete();
    }
    
    @DynamoDBTable(tableName = EXAMPLE_TABLE_NAME)
    public static class ExampleItem {

        private String itemId;
        private String value;
        private Long version;

        @DynamoDBHashKey(attributeName = "ItemId")
        public String getItemId() {
            return itemId;
        }

        public void setItemId(String itemId) {
            this.itemId = itemId;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @DynamoDBVersionAttribute
        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

    }

    /**
     * This example shows how to conditionally create or update an item in a transaction.
     */
    public void conditionallyCreateOrUpdateWithMapper() {
        print("\n*** conditionallyCreateOrUpdateWithMapper() ***\n");

        Transaction t1 = txManager.newTransaction();

        print("Reading Item1");
        ExampleItem keyItem = new ExampleItem();
        keyItem.setItemId("Item1");

        // Performs a GetItem request on the transaction
        ExampleItem item = t1.load(keyItem);
        if (item != null) {
            print("Item1: " + item.getValue());
            print("Item1 version: " + item.getVersion());

            print("Updating Item1");
            item.setValue("Magenta");

            // Performs an UpdateItem request after verifying the version is unchanged as of this transaction
            t1.save(item);
            print("Item1 is now: " + item.getValue());
            print("Item1 version is now: " + item.getVersion());
        } else {
            print("Creating Item1");
            item = new ExampleItem();
            item.setItemId(keyItem.getItemId());
            item.setValue("Violet");

            // Performs a CreateItem request after verifying the version attribute is not set as of this transaction
            t1.save(item);

            print("Item1 is now: " + item.getValue());
            print("Item1 version is now: " + item.getVersion());
        }

        t1.commit();
        t1.delete();
    }

    /**
     * Demonstrates the 3 levels of supported read isolation: Uncommitted, Committed, Locked
     */
    public void reading() {
        print("\n*** reading() ***\n");
        
        // First, create a new transaction and update Item1, but don't commit yet.
        print("Starting a transaction to modify Item1");
        Transaction t1 = txManager.newTransaction();
        
        Map<String, AttributeValue> key1 = new HashMap<String, AttributeValue>();
        key1.put(EXAMPLE_TABLE_HASH_KEY, new AttributeValue("Item1"));
        
        // Show the current value before any changes
        print("Getting the current value of Item1 within the transaction.  This is the strongest form of read isolation.");
        print("  However, you can't trust the value you get back until your transaction commits!");
        Map<String, AttributeValue> item1 = t1.getItem(new GetItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)).getItem();
        print("Before any changes, Item1 is: " + item1);
    
        // Show the current value before any changes
        print("Changing the Color of Item1, but not committing yet");
        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("Color", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("Purple")));
        
        item1 = t1.updateItem(new UpdateItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        print("Item1 is not yet committed, but if committed, will be: " + item1);
        
        // Perform an Uncommitted read
        print("The weakest (but cheapest) form of read is Uncommitted, where you can get back changes that aren't yet committed");
        print("  And might be rolled back!");
        item1 = txManager.getItem(new GetItemRequest() // Uncommitted reads happen on the transaction manager, not on a transaction.
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME), 
            IsolationLevel.UNCOMMITTED).getItem(); // Note the isloationLevel
        print("The read, which could return changes that will be rolled back, returned: " + item1);
        
        // Perform a Committed read
        print("A strong read is Committed.  This means that you are guaranteed to read only committed changes,");
        print("  but not necessarily the *latest* committed change!");
        item1 = txManager.getItem(new GetItemRequest() // Uncommitted reads happen on the transaction manager, not on a transaction.
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME), 
            IsolationLevel.COMMITTED).getItem(); // Note the isloationLevel
        print("The read, which should return the same value of the original read, returned: " + item1);
        
        // Now start a new transaction and do a read, write, and read in it
        Transaction t2 = txManager.newTransaction();
        
        print("Getting Item1, but this time under a new transaction t2");
        item1 = t2.getItem(new GetItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)).getItem();
        print("Before any changes, in t2, Item1 is: " + item1);
        print(" This just rolled back transaction t1! Notice that this is the same value as *before* t1!");
    
        updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("Color", new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue("Magenta")));
        
        print("Updating item1 again, but now under t2");
        item1 = t2.updateItem(new UpdateItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)
            .withAttributeUpdates(updates)
            .withReturnValues(ReturnValue.ALL_NEW)).getAttributes();
        print("Item1 is not yet committed, but if committed, will now be: " + item1);
        
        print("Getting Item1, again, under lock in t2.  Notice that the transaction library makes your write during this transaction visible to future reads.");
        item1 = t2.getItem(new GetItemRequest()
            .withKey(key1)
            .withTableName(EXAMPLE_TABLE_NAME)).getItem();
        print("Under transaction t2, Item1 is going to be: " + item1);
        
        print("Committing t2");
        t2.commit();
        
        try {
            print("Committing t1 (this will fail because it was rolled back)");
            t1.commit();   
            throw new RuntimeException("Should have been rolled back");
        } catch (TransactionRolledBackException e) {
            print("t1 was rolled back as expected.  I hope you didn't act on the GetItem you did under the lock in t1!");
        }
    }
    
    /**
     * Demonstrates reading with COMMITTED isolation level using the mapper.
     */
    public void readCommittedWithMapper() {
        print("\n*** readCommittedWithMapper() ***\n");

        print("Reading Item1 with IsolationLevel.COMMITTED");
        ExampleItem keyItem = new ExampleItem();
        keyItem.setItemId("Item1");
        ExampleItem item = txManager.load(keyItem, IsolationLevel.COMMITTED);

        print("Committed value of Item1: " + item.getValue());
    }

    public void sweepForStuckAndOldTransactions() {
        print("\n*** sweepForStuckAndOldTransactions() ***\n");
        
        // The scan should be done in a loop to follow the LastEvaluatedKey, and done with following the best practices for scanning a table.
        // This includes sleeping between pages, using Limit to limit the throughput of each operation to avoid hotspots,
        // and using parallel scan.
        print("Scanning one full page of the transactions table");
        ScanResult result = dynamodb.scan(new ScanRequest()
            .withTableName(TX_TABLE_NAME));
        
        // Pick some duration where transactions should be rolled back if they were sitting there PENDING.
        // 
        //long rollbackAfterDurationMills = 5 * 60 * 1000; // Must be idle and PENDING for 5 minutes to be rolled back
        //long deleteAfterDurationMillis = 24 * 60 * 60 * 1000; // Must be completed for 24 hours to be deleted
        long rollbackAfterDurationMills = 1;
        long deleteAfterDurationMillis = 1;
        for(Map<String, AttributeValue> txItem : result.getItems()) {
            print("Sweeping transaction " + txItem);
            try {
                if(TransactionManager.isTransactionItem(txItem)) {
                    Transaction t = txManager.resumeTransaction(txItem);    
                    t.sweep(rollbackAfterDurationMills, deleteAfterDurationMillis);
                    print("  - Swept transaction (but it might have been skipped)");
                }
            } catch (TransactionException e) {
                // Log and report an error "unsticking" this transaction, but keep going.
                print("  - Error sweeping transaction " + txItem + " " + e);
            }
        }
        
    }
    
    private static void print(CharSequence line) {
        System.out.println(line.toString());
    }
}
