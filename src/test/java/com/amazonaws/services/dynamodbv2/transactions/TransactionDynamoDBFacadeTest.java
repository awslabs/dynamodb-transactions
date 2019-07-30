/**
 * Copyright 2014-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.transactions;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;

public class TransactionDynamoDBFacadeTest {

    @Test
    public void testCheckExpectedStringValueWithMatchingItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue("Bar"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue("Bar")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
        // no exception expected
    }

    @Test(expected = ConditionalCheckFailedException.class)
    public void testCheckExpectedStringValueWithNonMatchingItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue("Bar"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue("NotBar")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
    }

    @Test
    public void testCheckExpectedBinaryValueWithMatchingItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 1, 127, -127 })));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 1, 127, -127 }))));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
        // no exception expected
    }

    @Test(expected = ConditionalCheckFailedException.class)
    public void testCheckExpectedBinaryValueWithNonMatchingItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 1, 127, -127 })));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 0, 127, -127 }))));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
    }

    @Test
    public void testCheckExpectedNumericValueWithMatchingItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue().withN("3.14"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withN("3.14")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
        // no exception expected
    }

    @Test
    public void testCheckExpectedNumericValueWithMatchingNotStringEqualItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue().withN("3.140"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withN("3.14")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
        // no exception expected
    }

    @Test(expected = ConditionalCheckFailedException.class)
    public void testCheckExpectedNumericValueWithNonMatchingItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue().withN("3.14"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withN("12")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
    }

    @Test(expected = ConditionalCheckFailedException.class)
    public void testCheckExpectedNumericValueWithStringTypedItem() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue("3.14"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withN("3.14")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckExpectedInvalidNumericValue() {
        Map<String, AttributeValue> item = Collections.singletonMap("Foo", new AttributeValue().withN("1.1"));
        Map<String, ExpectedAttributeValue> expected = Collections.singletonMap("Foo", new ExpectedAttributeValue(new AttributeValue().withN("!!.!!")));

        TransactionDynamoDBFacade.checkExpectedValues(expected, item);
    }

}
