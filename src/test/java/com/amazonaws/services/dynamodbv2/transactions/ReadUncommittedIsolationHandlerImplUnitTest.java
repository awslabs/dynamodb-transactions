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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class ReadUncommittedIsolationHandlerImplUnitTest {

    protected static final String TABLE_NAME = "TEST_TABLE";
    protected static final Map<String, AttributeValue> KEY = Collections.singletonMap("Id", new AttributeValue().withS("KeyValue"));
    protected static final String TX_ID = "e1b52a78-0187-4787-b1a3-27f63a78898b";
    protected static final Map<String, AttributeValue> UNLOCKED_ITEM = createItem(false, false, false);
    protected static final Map<String, AttributeValue> TRANSIENT_UNAPPLIED_ITEM = createItem(true, true, false);
    protected static final Map<String, AttributeValue> TRANSIENT_APPLIED_ITEM = createItem(true, true, true);
    protected static final Map<String, AttributeValue> NON_TRANSIENT_APPLIED_ITEM = createItem(true, false, true);

    private ReadUncommittedIsolationHandlerImpl isolationHandler;

    @Before
    public void setup() {
        isolationHandler = new ReadUncommittedIsolationHandlerImpl();
    }

    private static Map<String, AttributeValue> createItem(boolean isLocked, boolean isTransient, boolean isApplied) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        if (isLocked) {
            item.put(Transaction.AttributeName.TXID.toString(), new AttributeValue(TX_ID));
            item.put(Transaction.AttributeName.DATE.toString(), new AttributeValue().withS(""));
            if (isTransient) {
                item.put(Transaction.AttributeName.TRANSIENT.toString(), new AttributeValue().withS(""));
            }
            if (isApplied) {
                item.put(Transaction.AttributeName.APPLIED.toString(), new AttributeValue().withS(""));
            }
        }
        if (!isTransient) {
            item.put("attr1", new AttributeValue().withS("some value"));
        }
        item.putAll(KEY);
        return item;
    }

    @Test
    public void handleItemReturnsNullForNullItem() {
        assertNull(isolationHandler.handleItem(null, null, TABLE_NAME));
    }

    @Test
    public void handleItemReturnsItemForUnlockedItem() {
        assertEquals(UNLOCKED_ITEM, isolationHandler.handleItem(UNLOCKED_ITEM, null, TABLE_NAME));
    }

    @Test
    public void handleItemReturnsNullForTransientUnappliedItem() {
        assertNull(isolationHandler.handleItem(TRANSIENT_UNAPPLIED_ITEM, null, TABLE_NAME));
    }

    @Test
    public void handleItemReturnsNullForTransientAppliedItem() {
        assertEquals(TRANSIENT_APPLIED_ITEM, isolationHandler.handleItem(TRANSIENT_APPLIED_ITEM, null, TABLE_NAME));
    }

    @Test
    public void handleItemReturnsItemForNonTransientAppliedItem() {
        assertEquals(NON_TRANSIENT_APPLIED_ITEM, isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, null, TABLE_NAME));
    }

}
