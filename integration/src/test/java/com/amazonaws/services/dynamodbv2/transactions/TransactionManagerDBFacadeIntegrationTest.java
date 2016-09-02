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
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TransactionManagerDBFacadeIntegrationTest extends IntegrationTest {

    private TransactionManagerDynamoDBFacade uncommittedFacade;
    private TransactionManagerDynamoDBFacade committedFacade;

    private Map<String, AttributeValueUpdate> update;
    private Map<String, AttributeValue> item0Updated;
    private Map<String, AttributeValue> item0Filtered; // item0 with only the attributesToGet
    private List<String> attributesToGet;

    public TransactionManagerDBFacadeIntegrationTest() {
        super();
    }

    @Before
    public void setup() {
        dynamodb.reset();
        uncommittedFacade = new TransactionManagerDynamoDBFacade(manager, Transaction.IsolationLevel.UNCOMMITTED);
        committedFacade = new TransactionManagerDynamoDBFacade(manager, Transaction.IsolationLevel.COMMITTED);
        key0 = newKey(INTEG_HASH_TABLE_NAME);
        item0 = new HashMap<String, AttributeValue>(key0);
        item0.put("s_someattr", new AttributeValue("val"));
        item0Filtered = new HashMap<String, AttributeValue>(item0);
        item0.put("attr_not_to_get", new AttributeValue("val_not_to_get"));
        attributesToGet = Arrays.asList(ID_ATTRIBUTE, "s_someattr"); // not including attr_not_to_get
        update = Collections.singletonMap(
                "s_someattr",
                new AttributeValueUpdate().withValue(new AttributeValue("val2")));
        item0Updated = new HashMap<String, AttributeValue>(item0);
        item0Updated.put("s_someattr", new AttributeValue("val2"));
    }

    @After
    public void cleanup() throws InterruptedException {
        deleteTables();
        createTables();
        dynamodb.reset();
    }

    private void putItem(final boolean commit) {
        Transaction t = manager.newTransaction();
        t.putItem(new PutItemRequest()
                .withTableName(INTEG_HASH_TABLE_NAME)
                .withItem(item0));
        if (commit) {
            t.commit();
            assertItemNotLocked(INTEG_HASH_TABLE_NAME, key0, true);
        } else {
            assertItemLocked(INTEG_HASH_TABLE_NAME, key0, t.getId(), true, true);
        }
    }

    private void updateItem(final boolean commit) {
        Transaction t = manager.newTransaction();
        UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(INTEG_HASH_TABLE_NAME)
                .withKey(key0)
                .withAttributeUpdates(update);
        t.updateItem(request);
        if (commit) {
            t.commit();
            assertItemNotLocked(INTEG_HASH_TABLE_NAME, key0, true);
        } else {
            assertItemLocked(INTEG_HASH_TABLE_NAME, key0, t.getId(), false, true);
        }
    }

    private void assertContainsNoTransactionAttributes(final Map<String, AttributeValue> item) {
        assertFalse(Transaction.isLocked(item));
        assertFalse(Transaction.isApplied(item));
        assertFalse(Transaction.isTransient(item));
    }

    private QueryRequest createQueryRequest(final boolean filterAttributes) {
        Condition hashKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(key0.get(ID_ATTRIBUTE));
        QueryRequest request = new QueryRequest()
                .withTableName(INTEG_HASH_TABLE_NAME)
                .withKeyConditions(Collections.singletonMap(ID_ATTRIBUTE, hashKeyCondition));
        if (filterAttributes) {
            request.setAttributesToGet(attributesToGet);
        }
        return request;
    }

    private BatchGetItemRequest createBatchGetItemRequest(final boolean filterAttributes) {
        KeysAndAttributes keysAndAttributes = new KeysAndAttributes()
                .withKeys(key0);
        if (filterAttributes) {
            keysAndAttributes.withAttributesToGet(attributesToGet);
        }
        return new BatchGetItemRequest()
                .withRequestItems(
                        Collections.singletonMap(
                                INTEG_HASH_TABLE_NAME,
                                keysAndAttributes));
    }

    private void testGetItemContainsItem(
            final TransactionManagerDynamoDBFacade facade,
            final Map<String, AttributeValue> item,
            final boolean filterAttributes) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(INTEG_HASH_TABLE_NAME)
                .withKey(key0);
        if (filterAttributes) {
            request.setAttributesToGet(attributesToGet);
        }
        GetItemResult result = facade.getItem(request);
        assertContainsNoTransactionAttributes(result.getItem());
        assertEquals(item, result.getItem());
    }

    private void testScanContainsItem(
            final TransactionManagerDynamoDBFacade facade,
            final Map<String, AttributeValue> item,
            final boolean filterAttributes) {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(INTEG_HASH_TABLE_NAME);
        if (filterAttributes) {
            scanRequest.setAttributesToGet(attributesToGet);
        }
        ScanResult scanResult = facade.scan(scanRequest);
        assertEquals(1, scanResult.getItems().size());
        assertContainsNoTransactionAttributes(scanResult.getItems().get(0));
        assertEquals(item, scanResult.getItems().get(0));
    }

    private void testScanIsEmpty(final TransactionManagerDynamoDBFacade facade) {
        ScanResult scanResult = facade.scan(new ScanRequest()
                .withTableName(INTEG_HASH_TABLE_NAME));
        assertNotNull(scanResult.getItems());
        assertEquals(0, scanResult.getItems().size());
    }

    private void testQueryContainsItem(
            final TransactionManagerDynamoDBFacade facade,
            final Map<String, AttributeValue> item,
            final boolean filterAttributes) {
        QueryRequest queryRequest = createQueryRequest(filterAttributes);
        QueryResult queryResult = facade.query(queryRequest);
        assertEquals(1, queryResult.getItems().size());
        assertContainsNoTransactionAttributes(queryResult.getItems().get(0));
        assertEquals(item, queryResult.getItems().get(0));
    }

    private void testQueryIsEmpty(final TransactionManagerDynamoDBFacade facade) {
        QueryRequest queryRequest = createQueryRequest(false);
        QueryResult queryResult = facade.query(queryRequest);
        assertNotNull(queryResult.getItems());
        assertEquals(0, queryResult.getItems().size());
    }

    private void testBatchGetItemsContainsItem(
            final TransactionManagerDynamoDBFacade facade,
            final Map<String, AttributeValue> item,
            final boolean filterAttributes) {
        BatchGetItemRequest batchGetItemRequest = createBatchGetItemRequest(filterAttributes);
        BatchGetItemResult batchGetItemResult = facade.batchGetItem(batchGetItemRequest);
        List<Map<String, AttributeValue>> items = batchGetItemResult.getResponses().get(INTEG_HASH_TABLE_NAME);
        assertEquals(1, items.size());
        assertContainsNoTransactionAttributes(items.get(0));
        assertEquals(item, items.get(0));
    }

    private void testBatchGetItemsIsEmpty(final TransactionManagerDynamoDBFacade facade) {
        BatchGetItemRequest batchGetItemRequest = createBatchGetItemRequest(false);
        BatchGetItemResult batchGetItemResult = facade.batchGetItem(batchGetItemRequest);
        assertNotNull(batchGetItemResult.getResponses());
        assertEquals(1, batchGetItemResult.getResponses().size());
        assertNotNull(batchGetItemResult.getResponses().get(INTEG_HASH_TABLE_NAME));
        assertEquals(0, batchGetItemResult.getResponses().get(INTEG_HASH_TABLE_NAME).size());

    }

    /**
     * Test that calls to scan, query, getItem, and batchGetItems contain
     * the expected result.
     * @param facade The facade to test
     * @param item The expected item to be found
     * @param filterAttributes Whether or not to filter attributes using attributesToGet
     */
    private void testReadCallsContainItem(
            final TransactionManagerDynamoDBFacade facade,
            final Map<String, AttributeValue> item,
            final boolean filterAttributes) {

        // GetItem contains the expected result
        testGetItemContainsItem(facade, item, filterAttributes);

        // Scan contains the expected result
        testScanContainsItem(facade, item, filterAttributes);

        // Query contains the expected result
        testQueryContainsItem(facade, item, filterAttributes);

        // BatchGetItems contains the expected result
        testBatchGetItemsContainsItem(facade, item, filterAttributes);
    }

    private void testReadCallsReturnEmpty(final TransactionManagerDynamoDBFacade facade) {

        // GetItem contains null
        testGetItemContainsItem(facade, null, false);

        // Scan returns empty
        testScanIsEmpty(facade);

        // Query returns empty
        testQueryIsEmpty(facade);

        // BatchGetItems does not return item
        testBatchGetItemsIsEmpty(facade);
    }

    @Test
    public void uncommittedFacadeReadsItemIfCommitted() {
        putItem(true);

        // test that read calls contain the committed item
        testReadCallsContainItem(uncommittedFacade, item0, false);

        // test that read calls contain the committed item respecting attributesToGet
        testReadCallsContainItem(uncommittedFacade, item0Filtered, true);
    }

    @Test
    public void uncommittedFacadeReadsItemIfNotCommitted() {
        putItem(false);

        // test that read calls contain the uncommitted item
        testReadCallsContainItem(uncommittedFacade, item0, false);

        // test that read calls contain the uncommitted item respecting attributesToGet
        testReadCallsContainItem(uncommittedFacade, item0Filtered, true);
    }

    @Test
    public void uncommittedFacadeReadsUncommittedUpdate() {
        putItem(true);
        updateItem(false);

        // test that read calls contain the updated uncommitted item
        testReadCallsContainItem(uncommittedFacade, item0Updated, false);
    }

    @Test
    public void committedFacadeReadsCommittedItem() {
        putItem(true);

        // test that read calls contain the committed item
        testReadCallsContainItem(committedFacade, item0, false);

        // test that read calls contain the committed item respecting attributesToGet
        testReadCallsContainItem(committedFacade, item0Filtered, true);
    }

    @Test
    public void committedFacadeDoesNotReadUncommittedItem() {
        putItem(false);

        // test that read calls do not contain the uncommitted item
        testReadCallsReturnEmpty(committedFacade);
    }

    @Test
    public void committedFacadeDoesNotReadUncommittedUpdate() {
        putItem(true);
        updateItem(false);

        // test that read calls contain the last committed version of the item
        testReadCallsContainItem(committedFacade, item0, false);

        // test that read calls contain the last committed version of the item
        // respecting attributesToGet
        testReadCallsContainItem(committedFacade, item0Filtered, true);
    }

}
