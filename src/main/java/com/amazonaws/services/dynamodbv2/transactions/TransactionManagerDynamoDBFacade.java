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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
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
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
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
import com.amazonaws.services.dynamodbv2.transactions.Transaction.IsolationLevel;

/**
 * Facade to support the DynamoDBMapper doing a read using a specific isolation
 * level. Used by {@link TransactionManager#load(Object, IsolationLevel)}.
 */
public class TransactionManagerDynamoDBFacade implements AmazonDynamoDB {

    private final TransactionManager txManager;
    private final IsolationLevel isolationLevel;

    public TransactionManagerDynamoDBFacade(TransactionManager txManager, IsolationLevel isolationLevel) {
        this.txManager = txManager;
        this.isolationLevel = isolationLevel;
    }

    @Override
    public GetItemResult getItem(GetItemRequest request)
            throws AmazonServiceException, AmazonClientException {
        return txManager.getItem(request, isolationLevel);
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
    public BatchGetItemResult batchGetItem(BatchGetItemRequest arg0)
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
    public QueryResult query(QueryRequest arg0) throws AmazonServiceException,
            AmazonClientException {
        throw new UnsupportedOperationException("Use the underlying client instance instead");
    }

    @Override
    public ScanResult scan(ScanRequest arg0) throws AmazonServiceException,
            AmazonClientException {
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

}
