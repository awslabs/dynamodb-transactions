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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.transactions.Transaction.AttributeName;
import com.amazonaws.services.dynamodbv2.transactions.Transaction.IsolationLevel;
import com.amazonaws.services.dynamodbv2.util.TableHelper;

/**
 * A factory for client-side transactions on DynamoDB.  Thread-safe. 
 */
public class TransactionManager {
    
    private static final Log log = LogFactory.getLog(TransactionManager.class);
    private static final List<AttributeDefinition> TRANSACTIONS_TABLE_ATTRIBUTES;
    private static final List<KeySchemaElement> TRANSACTIONS_TABLE_KEY_SCHEMA = Collections.unmodifiableList(
        Arrays.asList(
            new KeySchemaElement().withAttributeName(AttributeName.TXID.toString()).withKeyType(KeyType.HASH)));
    
    private static final List<AttributeDefinition> TRANSACTION_IMAGES_TABLE_ATTRIBUTES;
    private static final List<KeySchemaElement> TRANSACTION_IMAGES_TABLE_KEY_SCHEMA = Collections.unmodifiableList(
        Arrays.asList(
            new KeySchemaElement().withAttributeName(AttributeName.IMAGE_ID.toString()).withKeyType(KeyType.HASH)));

    static {
        List<AttributeDefinition> definition = Arrays.asList( 
            new AttributeDefinition().withAttributeName(AttributeName.TXID.toString()).withAttributeType(ScalarAttributeType.S));
        Collections.sort(definition, new AttributeDefinitionComparator());
        TRANSACTIONS_TABLE_ATTRIBUTES = Collections.unmodifiableList(definition);
        
        definition = Arrays.asList( 
            new AttributeDefinition().withAttributeName(AttributeName.IMAGE_ID.toString()).withAttributeType(ScalarAttributeType.S));
        Collections.sort(definition, new AttributeDefinitionComparator());
        TRANSACTION_IMAGES_TABLE_ATTRIBUTES = Collections.unmodifiableList(definition);
    }
        
    private final AmazonDynamoDB client;
    private final String transactionTableName;
    private final String itemImageTableName;
    private final ConcurrentHashMap<String, List<KeySchemaElement>> tableSchemaCache = new ConcurrentHashMap<String, List<KeySchemaElement>>();
    private final DynamoDBMapper clientMapper;
    private final ThreadLocalDynamoDBFacade facadeProxy;
    
    public TransactionManager(AmazonDynamoDB client, String transactionTableName, String itemImageTableName) {
    	this(client, transactionTableName, itemImageTableName, DynamoDBMapperConfig.DEFAULT);
    }

    public TransactionManager(AmazonDynamoDB client, String transactionTableName, String itemImageTableName, DynamoDBMapperConfig config) {
        if(client == null) {
            throw new IllegalArgumentException("client must not be null");
        }
        if(transactionTableName == null) {
            throw new IllegalArgumentException("transactionTableName must not be null");
        }
        if(itemImageTableName == null) {
            throw new IllegalArgumentException("itemImageTableName must not be null");
        }
        this.client = client;
        this.transactionTableName = transactionTableName;
        this.itemImageTableName = itemImageTableName;
        this.facadeProxy = new ThreadLocalDynamoDBFacade();
        this.clientMapper = new DynamoDBMapper(facadeProxy, config);
    }
    
    protected List<KeySchemaElement> getTableSchema(String tableName) throws ResourceNotFoundException {
        List<KeySchemaElement> schema = tableSchemaCache.get(tableName);
        if(schema == null) {
            DescribeTableResult result = client.describeTable(new DescribeTableRequest().withTableName(tableName));
            schema = Collections.unmodifiableList(result.getTable().getKeySchema());
            tableSchemaCache.put(tableName, schema);
        }
        return schema;
    }
    
    public Transaction newTransaction() {
        Transaction transaction = new Transaction(UUID.randomUUID().toString(), this, true);
        log.info("Started transaction " + transaction.getId());
        return transaction;
    }
    
    public Transaction resumeTransaction(String txId) {
        Transaction transaction = new Transaction(txId, this, false);
        log.info("Resuming transaction from id " + transaction.getId());
        return transaction;
    }
    
    public Transaction resumeTransaction(Map<String, AttributeValue> txItem) {
        Transaction transaction = new Transaction(txItem, this);
        log.info("Resuming transaction from item " + transaction.getId());
        return transaction;
    }
    
    public static boolean isTransactionItem(Map<String, AttributeValue> txItem) {
        return TransactionItem.isTransactionItem(txItem);
    }

    public AmazonDynamoDB getClient() {
        return client;
    }

    public DynamoDBMapper getClientMapper() {
        return clientMapper;
    }

    protected ThreadLocalDynamoDBFacade getFacadeProxy() {
        return facadeProxy;
    }

    public GetItemResult getItem(GetItemRequest request, IsolationLevel isolationLevel) {
        return Transaction.getItem(request, isolationLevel, this);
    }
    
    public String getTransactionTableName() {
        return transactionTableName;
    }
    
    public String getItemImageTableName() {
        return itemImageTableName;
    }
    
    /**
     * Breaks an item lock and leaves the item intact, leaving an item in an unknown state.  Only works if the owning transaction
     * does not exist. 
     * 
     *   1) It could leave an item that should not exist (was inserted only for obtaining the lock)
     *   2) It could replace the item with an old copy of the item from an unknown previous transaction
     *   3) A request from an earlier transaction could be applied a second time
     *   4) Other conditions of this nature 
     * 
     * @param tableName
     * @param item
     * @param txId
     */
    public void breakLock(String tableName, Map<String, AttributeValue> item, String txId) {
        if(log.isWarnEnabled()) {
            log.warn("Breaking a lock on table " + tableName + " for transaction " + txId + " for item " + item + ".  This will leave the item in an unknown state");
        }
        Transaction.unlockItemUnsafe(this, tableName, item, txId);
    }

    public static void verifyOrCreateTransactionTable(AmazonDynamoDB client, String tableName, long readCapacityUnits, long writeCapacityUnits, Long waitTimeSeconds) throws InterruptedException {
        new TableHelper(client).verifyOrCreateTable(
            tableName, 
            TRANSACTIONS_TABLE_ATTRIBUTES, 
            TRANSACTIONS_TABLE_KEY_SCHEMA, 
            null/*localIndexes*/, 
            new ProvisionedThroughput()
                .withReadCapacityUnits(readCapacityUnits)
                .withWriteCapacityUnits(writeCapacityUnits), 
            waitTimeSeconds);
    }
    
    public static void verifyOrCreateTransactionImagesTable(AmazonDynamoDB client, String tableName, long readCapacityUnits, long writeCapacityUnits, Long waitTimeSeconds) throws InterruptedException {
        new TableHelper(client).verifyOrCreateTable(
            tableName, 
            TRANSACTION_IMAGES_TABLE_ATTRIBUTES, 
            TRANSACTION_IMAGES_TABLE_KEY_SCHEMA, 
            null/*localIndexes*/, 
            new ProvisionedThroughput()
                .withReadCapacityUnits(readCapacityUnits)
                .withWriteCapacityUnits(writeCapacityUnits), 
            waitTimeSeconds);
    }
    
    /**
     * Ensures that the transaction table exists and has the correct schema.
     * 
     * @param client
     * @param transactionTableName
     * @param transactionImagesTableName
     * @throws ResourceInUseException if the table exists but has the wrong schema
     * @throws ResourceNotFoundException if the table does not exist
     */
    public static void verifyTransactionTablesExist(AmazonDynamoDB client, String transactionTableName, String transactionImagesTableName) {
        String state = new TableHelper(client).verifyTableExists(transactionTableName, TRANSACTIONS_TABLE_ATTRIBUTES, TRANSACTIONS_TABLE_KEY_SCHEMA, null/*localIndexes*/);
        if(! "ACTIVE".equals(state)) {
            throw new ResourceInUseException("Table " + transactionTableName + " is not ACTIVE");
        }
        
        state = new TableHelper(client).verifyTableExists(transactionImagesTableName, TRANSACTION_IMAGES_TABLE_ATTRIBUTES, TRANSACTION_IMAGES_TABLE_KEY_SCHEMA, null/*localIndexes*/);
        if(! "ACTIVE".equals(state)) {
            throw new ResourceInUseException("Table " + transactionImagesTableName + " is not ACTIVE");
        }
    }
    
    protected double getCurrentTime() {
        return System.currentTimeMillis() / 1000.0;
    }
    
    protected AttributeValue getCurrentTimeAttribute() {
        return new AttributeValue().withN(new Double(getCurrentTime()).toString());
    }
    
    private static class AttributeDefinitionComparator implements Comparator<AttributeDefinition> {
        
        @Override
        public int compare(AttributeDefinition arg0, AttributeDefinition arg1) {
            if(arg0 == null) 
                return -1;
            
            if(arg1 == null)
                return 1;
            
            int comp = arg0.getAttributeName().compareTo(arg1.getAttributeName());
            if(comp != 0)
                return comp;
            
            comp = arg0.getAttributeType().compareTo(arg1.getAttributeType());
            return comp;
        }
        
    }

    /**
     * Load an item outside a transaction using the mapper.
     *
     * @param item
     *            An item where the key attributes are populated; the key
     *            attributes from this item are used to form the GetItemRequest
     *            to retrieve the item.
     * @param isolationLevel
     *            The isolation level to use; this has the same meaning as for
     *            {@link TransactionManager#getItem(GetItemRequest, IsolationLevel)}
     *            .
     * @return An instance of the item class with all attributes populated from
     *         the table, or null if the item does not exist.
     */
    public <T> T load(T item,
            IsolationLevel isolationLevel) {
        try {
            getFacadeProxy().setBackend(new TransactionManagerDynamoDBFacade(this, isolationLevel));
            return getClientMapper().load(item);
        } finally {
            getFacadeProxy().setBackend(null);
        }
    }
}
