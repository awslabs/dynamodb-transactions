/**
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License"). You may not use
 * this file except in compliance with the License. A copy of the License is
 * located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or
 * implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.transactions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.transactions.Transaction.IsolationLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Facade to support the DynamoDBMapper doing a read using a specific isolation
 * level. Used by {@link TransactionManager#load(Object, IsolationLevel)}.
 */
public class TransactionManagerDynamoDBFacade extends AbstractAmazonDynamoDB {

    private final TransactionManager txManager;
    private final IsolationLevel isolationLevel;
    private final ReadIsolationHandler isolationHandler;

    public TransactionManagerDynamoDBFacade(TransactionManager txManager, IsolationLevel isolationLevel) {
        this.txManager = txManager;
        this.isolationLevel = isolationLevel;
        this.isolationHandler = txManager.getReadIsolationHandler(isolationLevel);
    }

    /**
     * Returns versions of the items can be read at the specified isolation level stripped of
     * special attributes.
     * @param items The items to check
     * @param tableName The table that contains the item
     * @param attributesToGet The attributes to get from the table. If null or empty, will
     *                        fetch all attributes.
     * @return Versions of the items that can be read at the isolation level stripped of special attributes
     */
    private List<Map<String, AttributeValue>> handleItems(
            final List<Map<String, AttributeValue>> items,
            final String tableName,
            final List<String> attributesToGet) {
        List<Map<String, AttributeValue>> result = new ArrayList<Map<String, AttributeValue>>();
        for (Map<String, AttributeValue> item : items) {
            Map<String, AttributeValue> handledItem = isolationHandler.handleItem(item, attributesToGet, tableName);
            /**
             * If the item is null, BatchGetItems, Scan, and Query should exclude the item from
             * the returned list. This is based on the DynamoDB documentation.
             */
            if (handledItem != null) {
                Transaction.stripSpecialAttributes(handledItem);
                result.add(handledItem);
            }
        }
        return result;
    }

    private Collection<String> addSpecialAttributes(Collection<String> attributesToGet) {
        if (attributesToGet == null) {
            return null;
        }
        Set<String> result = new HashSet<String>(attributesToGet);
        result.addAll(Transaction.SPECIAL_ATTR_NAMES);
        return result;
    }

    @Override
    public GetItemResult getItem(GetItemRequest request)
            throws AmazonServiceException, AmazonClientException {
        return txManager.getItem(request, isolationLevel);
    }

    @Override
    public GetItemResult getItem(
            String tableName,
            Map<String, AttributeValue> key) throws AmazonServiceException, AmazonClientException {
        return getItem(new GetItemRequest()
                .withTableName(tableName)
                .withKey(key));
    }

    @Override
    public GetItemResult getItem(String tableName,
            Map<String, AttributeValue> key, Boolean consistentRead)
            throws AmazonServiceException, AmazonClientException {
        return getItem(new GetItemRequest()
                .withTableName(tableName)
                .withKey(key)
                .withConsistentRead(consistentRead));
    }

    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest request)
            throws AmazonServiceException, AmazonClientException {
        for (KeysAndAttributes keysAndAttributes : request.getRequestItems().values()) {
            Collection<String> attributesToGet = keysAndAttributes.getAttributesToGet();
            keysAndAttributes.setAttributesToGet(addSpecialAttributes(attributesToGet));
        }
        BatchGetItemResult result = txManager.getClient().batchGetItem(request);
        Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<String, List<Map<String, AttributeValue>>>();
        for (Map.Entry<String, List<Map<String, AttributeValue>>> e : result.getResponses().entrySet()) {
            String tableName = e.getKey();
            List<String> attributesToGet = request.getRequestItems().get(tableName).getAttributesToGet();
            List<Map<String, AttributeValue>> items = handleItems(e.getValue(), tableName, attributesToGet);
            responses.put(tableName, items);
        }
        result.setResponses(responses);
        return result;
    }

    @Override
    public BatchGetItemResult batchGetItem(
            Map<String, KeysAndAttributes> requestItems,
            String returnConsumedCapacity) throws AmazonServiceException,
            AmazonClientException {
        BatchGetItemRequest request = new BatchGetItemRequest()
                .withRequestItems(requestItems)
                .withReturnConsumedCapacity(returnConsumedCapacity);
        return batchGetItem(request);
    }

    @Override
    public BatchGetItemResult batchGetItem(
            Map<String, KeysAndAttributes> requestItems)
            throws AmazonServiceException, AmazonClientException {
        BatchGetItemRequest request = new BatchGetItemRequest()
                .withRequestItems(requestItems);
        return batchGetItem(request);
    }

    @Override
    public ScanResult scan(ScanRequest request) throws AmazonServiceException,
            AmazonClientException {
        Collection<String> attributesToGet = addSpecialAttributes(request.getAttributesToGet());
        request.setAttributesToGet(attributesToGet);
        ScanResult result = txManager.getClient().scan(request);
        List<Map<String,AttributeValue>> items = handleItems(result.getItems(), request.getTableName(), request.getAttributesToGet());
        result.setItems(items);
        return result;
    }

    @Override
    public ScanResult scan(String tableName, List<String> attributesToGet)
            throws AmazonServiceException, AmazonClientException {
        ScanRequest request = new ScanRequest()
                .withTableName(tableName)
                .withAttributesToGet(attributesToGet);
        return scan(request);
    }

    @Override
    public ScanResult scan(String tableName, Map<String, Condition> scanFilter)
            throws AmazonServiceException, AmazonClientException {
        ScanRequest request = new ScanRequest()
                .withTableName(tableName)
                .withScanFilter(scanFilter);
        return scan(request);
    }

    @Override
    public ScanResult scan(
            String tableName,
            List<String> attributesToGet,
            Map<String, Condition> scanFilter) throws AmazonServiceException, AmazonClientException {
        ScanRequest request = new ScanRequest()
                .withTableName(tableName)
                .withAttributesToGet(attributesToGet)
                .withScanFilter(scanFilter);
        return scan(request);
    }

    @Override
    public QueryResult query(QueryRequest request) throws AmazonServiceException,
            AmazonClientException {
        Collection<String> attributesToGet = addSpecialAttributes(request.getAttributesToGet());
        request.setAttributesToGet(attributesToGet);
        QueryResult result = txManager.getClient().query(request);
        List<Map<String, AttributeValue>> items = handleItems(result.getItems(), request.getTableName(), request.getAttributesToGet());
        result.setItems(items);
        return result;
    }

    @Override
    public PutItemResult putItem(PutItemRequest request)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest request)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest request)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest arg0)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest arg0)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest arg0)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DescribeTableResult describeTable(DescribeTableRequest arg0)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(
            AmazonWebServiceRequest arg0) {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ListTablesResult listTables() throws AmazonServiceException,
            AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest arg0)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public void setEndpoint(String arg0) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public void setRegion(Region arg0) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest arg0)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public UpdateTableResult updateTable(String tableName,
            ProvisionedThroughput provisionedThroughput)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DeleteTableResult deleteTable(String tableName)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public BatchWriteItemResult batchWriteItem(
            Map<String, List<WriteRequest>> requestItems)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DescribeTableResult describeTable(String tableName)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DeleteItemResult deleteItem(String tableName,
            Map<String, AttributeValue> key) throws AmazonServiceException,
            AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public DeleteItemResult deleteItem(String tableName,
            Map<String, AttributeValue> key, String returnValues)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public CreateTableResult createTable(
            List<AttributeDefinition> attributeDefinitions, String tableName,
            List<KeySchemaElement> keySchema,
            ProvisionedThroughput provisionedThroughput)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public PutItemResult putItem(String tableName,
            Map<String, AttributeValue> item) throws AmazonServiceException,
            AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public PutItemResult putItem(String tableName,
            Map<String, AttributeValue> item, String returnValues)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ListTablesResult listTables(String exclusiveStartTableName)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ListTablesResult listTables(String exclusiveStartTableName,
            Integer limit) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ListTablesResult listTables(Integer limit)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public UpdateItemResult updateItem(String tableName,
            Map<String, AttributeValue> key,
            Map<String, AttributeValueUpdate> attributeUpdates)
            throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public UpdateItemResult updateItem(String tableName,
            Map<String, AttributeValue> key,
            Map<String, AttributeValueUpdate> attributeUpdates,
            String returnValues) throws AmazonServiceException,
            AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

}
