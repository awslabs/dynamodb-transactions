/**
 * Copyright 2014-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
