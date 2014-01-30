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
 package com.amazonaws.services.dynamodbv2.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

public class TableHelper {

    private final AmazonDynamoDB client;
    
    public TableHelper(AmazonDynamoDB client) {
        if(client == null) {
            throw new IllegalArgumentException("client must not be null");
        }
        this.client = client;
    }
    
    public String verifyTableExists( 
        String tableName, 
        List<AttributeDefinition> definitions, 
        List<KeySchemaElement> keySchema,
        List<LocalSecondaryIndex> localIndexes) {
        
        DescribeTableResult describe = client.describeTable(new DescribeTableRequest().withTableName(tableName));
        if(! new HashSet<AttributeDefinition>(definitions).equals(new HashSet<AttributeDefinition>(describe.getTable().getAttributeDefinitions()))) {
            throw new ResourceInUseException("Table " + tableName + " had the wrong AttributesToGet." 
                + " Expected: " + definitions + " "
                + " Was: " + describe.getTable().getAttributeDefinitions());
        }
        
        if(! keySchema.equals(describe.getTable().getKeySchema())) {
            throw new ResourceInUseException("Table " + tableName + " had the wrong KeySchema." 
                + " Expected: " + keySchema + " "
                + " Was: " + describe.getTable().getKeySchema());
        }
        
        List<LocalSecondaryIndex> theirLSIs = null;
        if(describe.getTable().getLocalSecondaryIndexes() != null) {
            theirLSIs = new ArrayList<LocalSecondaryIndex>();
            for(LocalSecondaryIndexDescription description : describe.getTable().getLocalSecondaryIndexes()) {
                LocalSecondaryIndex lsi = new LocalSecondaryIndex()
                    .withIndexName(description.getIndexName())
                    .withKeySchema(description.getKeySchema())
                    .withProjection(description.getProjection());
                theirLSIs.add(lsi);
            }
        }
        
        if(localIndexes != null) {
            if(! new HashSet<LocalSecondaryIndex>(localIndexes).equals(new HashSet<LocalSecondaryIndex>(theirLSIs))) {
                throw new ResourceInUseException("Table " + tableName + " did not have the expected LocalSecondaryIndexes."
                    + " Expected: " + localIndexes
                    + " Was: " + theirLSIs);
            }
        } else {
            if(theirLSIs != null) {
                throw new ResourceInUseException("Table " + tableName + " had local secondary indexes, but expected none."
                    + " Indexes: " + theirLSIs);
            }
        }

        return describe.getTable().getTableStatus();
    }
    
    /**
     * Verifies that the table exists with the specified schema, and creates it if it does not exist.
     * 
     * @param tableName
     * @param definitions
     * @param keySchema
     * @param localIndexes
     * @param provisionedThroughput
     * @param waitTimeSeconds
     * @throws InterruptedException 
     */
    public void verifyOrCreateTable(
        String tableName, 
        List<AttributeDefinition> definitions, 
        List<KeySchemaElement> keySchema,
        List<LocalSecondaryIndex> localIndexes,
        ProvisionedThroughput provisionedThroughput,
        Long waitTimeSeconds) throws InterruptedException {
        
        if(waitTimeSeconds != null && waitTimeSeconds < 0) {
            throw new IllegalArgumentException("Invalid waitTimeSeconds " + waitTimeSeconds);
        }
        
        String status = null;
        try {
            status = verifyTableExists(tableName, definitions, keySchema, localIndexes);
        } catch(ResourceNotFoundException e) {
            status = client.createTable(new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(definitions)
                .withKeySchema(keySchema)
                .withLocalSecondaryIndexes(localIndexes)
                .withProvisionedThroughput(provisionedThroughput)).getTableDescription().getTableStatus();
        }
        
        if(waitTimeSeconds != null && ! TableStatus.ACTIVE.toString().equals(status)) {
            waitForTableActive(tableName, definitions, keySchema, localIndexes, waitTimeSeconds);
        }
    }
    
    public void waitForTableActive(String tableName, long waitTimeSeconds) throws InterruptedException {
        if(waitTimeSeconds < 0) {
            throw new IllegalArgumentException("Invalid waitTimeSeconds " + waitTimeSeconds);
        }
        
        long startTimeMs = System.currentTimeMillis();
        long elapsedMs = 0;
        do {
            DescribeTableResult describe = client.describeTable(new DescribeTableRequest().withTableName(tableName));
            String status = describe.getTable().getTableStatus();
            if(TableStatus.ACTIVE.toString().equals(status)) {
                return;
            }
            if(TableStatus.DELETING.toString().equals(status)) {
                throw new ResourceInUseException("Table " + tableName + " is " + status + ", and waiting for it to become ACTIVE is not useful.");
            }
            Thread.sleep(10 * 1000);
            elapsedMs = System.currentTimeMillis() - startTimeMs; 
        } while(elapsedMs / 1000.0 < waitTimeSeconds);
        
        throw new ResourceInUseException("Table " + tableName + " did not become ACTIVE after " + waitTimeSeconds + " seconds.");
    }
    
    public void waitForTableActive(String tableName, 
        List<AttributeDefinition> definitions, 
        List<KeySchemaElement> keySchema,
        List<LocalSecondaryIndex> localIndexes,
        long waitTimeSeconds) throws InterruptedException {
        
        if(waitTimeSeconds < 0) {
            throw new IllegalArgumentException("Invalid waitTimeSeconds " + waitTimeSeconds);
        }
        
        long startTimeMs = System.currentTimeMillis();
        long elapsedMs = 0;
        do {
            String status = verifyTableExists(tableName, definitions, keySchema, localIndexes);
            if(TableStatus.ACTIVE.toString().equals(status)) {
                return;
            }
            if(TableStatus.DELETING.toString().equals(status)) {
                throw new ResourceInUseException("Table " + tableName + " is " + status + ", and waiting for it to become ACTIVE is not useful.");
            }
            Thread.sleep(10 * 1000);
            elapsedMs = System.currentTimeMillis() - startTimeMs; 
        } while(elapsedMs / 1000.0 < waitTimeSeconds);
        
        throw new ResourceInUseException("Table " + tableName + " did not become ACTIVE after " + waitTimeSeconds + " seconds.");
    }
    
    public void waitForTableDeleted(String tableName, long waitTimeSeconds) throws InterruptedException {
        
        if(waitTimeSeconds < 0) {
            throw new IllegalArgumentException("Invalid waitTimeSeconds " + waitTimeSeconds);
        }
        
        long startTimeMs = System.currentTimeMillis();
        long elapsedMs = 0;
        do {
            try {
                DescribeTableResult describe = client.describeTable(new DescribeTableRequest().withTableName(tableName));
                String status = describe.getTable().getTableStatus();
                if(! TableStatus.DELETING.toString().equals(status)) {
                    throw new ResourceInUseException("Table " + tableName + " is " + status + ", and waiting for it to not exist is only useful if it is DELETING.");
                }
            } catch (ResourceNotFoundException e) {
                return;
            }
            Thread.sleep(10 * 1000);
            elapsedMs = System.currentTimeMillis() - startTimeMs; 
        } while(elapsedMs / 1000.0 < waitTimeSeconds);
        
        throw new ResourceInUseException("Table " + tableName + " was not deleted after " + waitTimeSeconds + " seconds.");
    }
}
