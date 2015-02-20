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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.transactions.Request.DeleteItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.GetItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.PutItem;
import com.amazonaws.services.dynamodbv2.transactions.Request.UpdateItem;
import com.amazonaws.services.dynamodbv2.transactions.TransactionManager;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.InvalidRequestException;

public class RequestTest {

    private static final String TABLE_NAME = "Dummy";
    private static final String HASH_ATTR_NAME = "Foo";
    private static final List<KeySchemaElement> HASH_SCHEMA = Arrays.asList(
        new KeySchemaElement().withAttributeName(HASH_ATTR_NAME).withKeyType(KeyType.HASH));

    static final Map<String, AttributeValue> JSON_M_ATTR_VAL = new HashMap<String, AttributeValue>();
    private static final Map<String, ExpectedAttributeValue> NONNULL_EXPECTED_ATTR_VALUES = new HashMap<String, ExpectedAttributeValue>();
    private static final Map<String, String> NONNULL_EXP_ATTR_NAMES = new HashMap<String, String>();
    private static final Map<String, AttributeValue> NONNULL_EXP_ATTR_VALUES = new HashMap<String, AttributeValue>();
    private static final Map<String, AttributeValue> BASIC_ITEM = new HashMap<String, AttributeValue>();

    static {
        JSON_M_ATTR_VAL.put("attr_s", new AttributeValue().withS("s"));
        JSON_M_ATTR_VAL.put("attr_n", new AttributeValue().withN("1"));
        JSON_M_ATTR_VAL.put("attr_b", new AttributeValue().withB(ByteBuffer.wrap(new String("asdf").getBytes())));
        JSON_M_ATTR_VAL.put("attr_ss", new AttributeValue().withSS("a", "b"));
        JSON_M_ATTR_VAL.put("attr_ns", new AttributeValue().withNS("1", "2"));
        JSON_M_ATTR_VAL.put("attr_bs", new AttributeValue().withBS(ByteBuffer.wrap(new String("asdf").getBytes()), ByteBuffer.wrap(new String("ghjk").getBytes())));
        JSON_M_ATTR_VAL.put("attr_bool", new AttributeValue().withBOOL(true));
        JSON_M_ATTR_VAL.put("attr_l", new AttributeValue().withL(
            new AttributeValue().withS("s"),
            new AttributeValue().withN("1"),
            new AttributeValue().withB(ByteBuffer.wrap(new String("asdf").getBytes())),
            new AttributeValue().withBOOL(true),
            new AttributeValue().withNULL(true)));
        JSON_M_ATTR_VAL.put("attr_null", new AttributeValue().withNULL(true));

        BASIC_ITEM.put(HASH_ATTR_NAME, new AttributeValue("a"));
    }

    @Test
    public void validPut() {
        PutItem r = new PutItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        r.setRequest(new PutItemRequest()
            .withTableName(TABLE_NAME)
            .withItem(item));
        r.validate("1", new MockTransactionManager(HASH_SCHEMA));
    }

    @Test
    public void putNullTableName() {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));

        invalidRequestTest(new PutItemRequest()
                .withItem(item),
            "TableName must not be null");
    }

    @Test
    public void putNullItem() {
        invalidRequestTest(new PutItemRequest()
                .withTableName(TABLE_NAME),
            "PutItem must contain an Item");
    }

    @Test
    public void putMissingKey() {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("other-attr", new AttributeValue("a"));

        invalidRequestTest(new PutItemRequest()
                .withTableName(TABLE_NAME)
                .withItem(item),
            "PutItem request must contain the key attribute");
    }

    @Test
    public void putExpected() {
        invalidRequestTest(getBasicPutRequest()
                .withExpected(NONNULL_EXPECTED_ATTR_VALUES),
            "Requests with conditions");
    }

    @Test
    public void putConditionExpression() {
        invalidRequestTest(getBasicPutRequest()
                .withConditionExpression("attribute_not_exists (some_field)"),
            "Requests with conditions");
    }

    @Test
    public void putExpressionAttributeNames() {
        invalidRequestTest(getBasicPutRequest()
                .withExpressionAttributeNames(NONNULL_EXP_ATTR_NAMES),
            "Requests with expressions");
    }

    @Test
    public void putExpressionAttributeValues() {
        invalidRequestTest(getBasicPutRequest()
                .withExpressionAttributeValues(NONNULL_EXP_ATTR_VALUES),
            "Requests with expressions");
    }

    @Test
    public void updateExpected() {
        invalidRequestTest(getBasicUpdateRequest()
                .withExpected(NONNULL_EXPECTED_ATTR_VALUES),
            "Requests with conditions");
    }

    @Test
    public void updateConditionExpression() {
        invalidRequestTest(getBasicUpdateRequest()
                .withConditionExpression("attribute_not_exists(some_field)"),
            "Requests with conditions");
    }

    @Test
    public void updateUpdateExpression() {
        invalidRequestTest(getBasicUpdateRequest()
                .withUpdateExpression("REMOVE some_field"),
            "Requests with expressions");
    }

    @Test
    public void updateExpressionAttributeNames() {
        invalidRequestTest(getBasicUpdateRequest()
                .withExpressionAttributeNames(NONNULL_EXP_ATTR_NAMES),
            "Requests with expressions");
    }

    @Test
    public void updateExpressionAttributeValues() {
        invalidRequestTest(getBasicUpdateRequest()
                .withExpressionAttributeValues(NONNULL_EXP_ATTR_VALUES),
            "Requests with expressions");
    }

    @Test
    public void deleteExpected() {
        invalidRequestTest(getBasicDeleteRequest()
                .withExpected(NONNULL_EXPECTED_ATTR_VALUES),
            "Requests with conditions");
    }

    @Test
    public void deleteConditionExpression() {
        invalidRequestTest(getBasicDeleteRequest()
                .withConditionExpression("attribute_not_exists (some_field)"),
            "Requests with conditions");
    }

    @Test
    public void deleteExpressionAttributeNames() {
        invalidRequestTest(getBasicDeleteRequest()
                .withExpressionAttributeNames(NONNULL_EXP_ATTR_NAMES),
            "Requests with expressions");
    }

    @Test
    public void deleteExpressionAttributeValues() {
        invalidRequestTest(getBasicDeleteRequest()
                .withExpressionAttributeValues(NONNULL_EXP_ATTR_VALUES),
            "Requests with expressions");
    }

    @Test
    public void validUpdate() {
        UpdateItem r = new UpdateItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        r.setRequest(new UpdateItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(item));
        r.validate("1", new MockTransactionManager(HASH_SCHEMA));
    }

    @Test
    public void validDelete() {
        DeleteItem r = new DeleteItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        r.setRequest(new DeleteItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(item));
        r.validate("1", new MockTransactionManager(HASH_SCHEMA));
    }

    @Test
    public void validLock() {
        GetItem r = new GetItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        r.setRequest(new GetItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(item));
        r.validate("1", new MockTransactionManager(HASH_SCHEMA));
    }

    @Test
    public void roundTripGetString() {
        GetItem r1 = new GetItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        r1.setRequest(new GetItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(item));
        byte[] r1Bytes = Request.serialize("123", r1).array();
        Request r2 = Request.deserialize("123", ByteBuffer.wrap(r1Bytes));
        byte[] r2Bytes = Request.serialize("123", r2).array();
        assertArrayEquals(r1Bytes, r2Bytes);
    }

    @Test
    public void roundTripPutAll() {
        PutItem r1 = new PutItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        item.put("attr_ss", new AttributeValue().withSS("a", "b"));
        item.put("attr_n", new AttributeValue().withN("1"));
        item.put("attr_ns", new AttributeValue().withNS("1", "2"));
        item.put("attr_b", new AttributeValue().withB(ByteBuffer.wrap(new String("asdf").getBytes())));
        item.put("attr_bs", new AttributeValue().withBS(ByteBuffer.wrap(new String("asdf").getBytes()), ByteBuffer.wrap(new String("asdf").getBytes())));
        r1.setRequest(new PutItemRequest()
            .withTableName(TABLE_NAME)
            .withItem(item)
            .withReturnValues("ALL_OLD"));
        byte[] r1Bytes = Request.serialize("123", r1).array();
        Request r2 = Request.deserialize("123", ByteBuffer.wrap(r1Bytes));
        assertEquals(r1.getRequest(), ((PutItem)r2).getRequest());
        byte[] r2Bytes = Request.serialize("123", r2).array();
        assertArrayEquals(r1Bytes, r2Bytes);
    }

    @Test
    public void roundTripUpdateAll() {
        UpdateItem r1 = new UpdateItem();
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put(HASH_ATTR_NAME, new AttributeValue("a"));

        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("attr_ss", new AttributeValueUpdate().withAction("PUT").withValue(new AttributeValue().withSS("a", "b")));
        updates.put("attr_n", new AttributeValueUpdate().withAction("PUT").withValue(new AttributeValue().withN("1")));
        updates.put("attr_ns", new AttributeValueUpdate().withAction("PUT").withValue(new AttributeValue().withNS("1", "2")));
        updates.put("attr_b", new AttributeValueUpdate().withAction("PUT").withValue(new AttributeValue().withB(ByteBuffer.wrap(new String("asdf").getBytes()))));
        updates.put("attr_bs", new AttributeValueUpdate().withAction("PUT").withValue(new AttributeValue().withBS(ByteBuffer.wrap(new String("asdf").getBytes()), ByteBuffer.wrap(new String("asdf").getBytes()))));
        r1.setRequest(new UpdateItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(key)
            .withAttributeUpdates(updates));
        byte[] r1Bytes = Request.serialize("123", r1).array();
        Request r2 = Request.deserialize("123", ByteBuffer.wrap(r1Bytes));
        byte[] r2Bytes = Request.serialize("123", r2).array();
        assertArrayEquals(r1Bytes, r2Bytes);
    }

    @Test
    public void roundTripPutAllJSON() {
        PutItem r1 = new PutItem();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put(HASH_ATTR_NAME, new AttributeValue("a"));
        item.put("json_attr", new AttributeValue().withM(JSON_M_ATTR_VAL));
        r1.setRequest(new PutItemRequest()
            .withTableName(TABLE_NAME)
            .withItem(item)
            .withReturnValues("ALL_OLD"));
        byte[] r1Bytes = Request.serialize("123", r1).array();
        Request r2 = Request.deserialize("123", ByteBuffer.wrap(r1Bytes));
        assertEquals(r1.getRequest(), ((PutItem)r2).getRequest());
        byte[] r2Bytes = Request.serialize("123", r2).array();
        assertArrayEquals(r1Bytes, r2Bytes);
    }

    @Test
    public void roundTripUpdateAllJSON() {
        UpdateItem r1 = new UpdateItem();
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put(HASH_ATTR_NAME, new AttributeValue("a"));

        Map<String, AttributeValueUpdate> updates = new HashMap<String, AttributeValueUpdate>();
        updates.put("attr_m", new AttributeValueUpdate().withAction("PUT").withValue(new AttributeValue().withM(JSON_M_ATTR_VAL)));
        r1.setRequest(new UpdateItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(key)
            .withAttributeUpdates(updates));
        byte[] r1Bytes = Request.serialize("123", r1).array();
        Request r2 = Request.deserialize("123", ByteBuffer.wrap(r1Bytes));
        byte[] r2Bytes = Request.serialize("123", r2).array();
        assertArrayEquals(r1Bytes, r2Bytes);
    }

    private PutItemRequest getBasicPutRequest() {
        return new PutItemRequest().withItem(BASIC_ITEM).withTableName(TABLE_NAME);
    }

    private UpdateItemRequest getBasicUpdateRequest() {
        return new UpdateItemRequest().withKey(BASIC_ITEM).withTableName(TABLE_NAME);
    }

    private DeleteItemRequest getBasicDeleteRequest() {
        return new DeleteItemRequest().withKey(BASIC_ITEM).withTableName(TABLE_NAME);
    }

    private void invalidRequestTest(PutItemRequest request, String expectedExceptionMessage) {
        PutItem r = new PutItem();
        r.setRequest(request);
        try {
            r.validate("1", new MockTransactionManager(HASH_SCHEMA));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().contains(expectedExceptionMessage));
        }
    }

    private void invalidRequestTest(UpdateItemRequest request, String expectedExceptionMessage) {
        UpdateItem r = new UpdateItem();
        r.setRequest(request);
        try {
            r.validate("1", new MockTransactionManager(HASH_SCHEMA));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().contains(expectedExceptionMessage));
        }
    }

    private void invalidRequestTest(DeleteItemRequest request, String expectedExceptionMessage) {
        DeleteItem r = new DeleteItem();
        r.setRequest(request);
        try {
            r.validate("1", new MockTransactionManager(HASH_SCHEMA));
            fail();
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().contains(expectedExceptionMessage));
        }
    }

    protected class MockTransactionManager extends TransactionManager {

        private final List<KeySchemaElement> keySchema;

        public MockTransactionManager(List<KeySchemaElement> keySchema) {
            super(new AmazonDynamoDBClient(), "Dummy", "DummyOther");
            this.keySchema = keySchema;
        }

        @Override
        protected List<KeySchemaElement> getTableSchema(String tableName) throws ResourceNotFoundException {
            return keySchema;
        }
    }
}

