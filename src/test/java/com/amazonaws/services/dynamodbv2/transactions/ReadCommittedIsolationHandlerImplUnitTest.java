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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.transactions.TransactionItem.State;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionNotFoundException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.UnknownCompletedTransactionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.KEY;
import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.NON_TRANSIENT_APPLIED_ITEM;
import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.TABLE_NAME;
import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.TRANSIENT_APPLIED_ITEM;
import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.TRANSIENT_UNAPPLIED_ITEM;
import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.TX_ID;
import static com.amazonaws.services.dynamodbv2.transactions.ReadUncommittedIsolationHandlerImplUnitTest.UNLOCKED_ITEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReadCommittedIsolationHandlerImplUnitTest {

    protected static final int RID = 1;
    protected static GetItemRequest GET_ITEM_REQUEST = new GetItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(KEY)
            .withConsistentRead(true);

    @Mock
    private TransactionManager mockTxManager;

    @Mock
    private Transaction mockTx;

    @Mock
    private TransactionItem mockTxItem;

    @Mock
    private Request mockRequest;

    @Mock
    private AmazonDynamoDB mockClient;

    private ReadCommittedIsolationHandlerImpl isolationHandler;

    @Before
    public void setup() {
        isolationHandler = spy(new ReadCommittedIsolationHandlerImpl(mockTxManager, 0));
        when(mockTx.getTxItem()).thenReturn(mockTxItem);
        when(mockTx.getId()).thenReturn(TX_ID);
        when(mockTxManager.getClient()).thenReturn(mockClient);
    }

    @Test
    public void checkItemCommittedReturnsNullForNullItem() {
        assertNull(isolationHandler.checkItemCommitted(null));
    }

    @Test
    public void checkItemCommittedReturnsItemForUnlockedItem() {
        assertEquals(UNLOCKED_ITEM, isolationHandler.checkItemCommitted(UNLOCKED_ITEM));
    }

    @Test
    public void checkItemCommittedReturnsNullForTransientItem() {
        assertNull(isolationHandler.checkItemCommitted(TRANSIENT_APPLIED_ITEM));
        assertNull(isolationHandler.checkItemCommitted(TRANSIENT_UNAPPLIED_ITEM));
    }

    @Test(expected = TransactionException.class)
    public void checkItemCommittedThrowsExceptionForNonTransientAppliedItem() {
        isolationHandler.checkItemCommitted(NON_TRANSIENT_APPLIED_ITEM);
    }

    @Test
    public void filterAttributesToGetReturnsNullForNullItem() {
        isolationHandler.filterAttributesToGet(null, null);
    }

    @Test
    public void filterAttributesToGetReturnsItemWhenAttributesToGetIsNull() {
        Map<String, AttributeValue> result = isolationHandler.filterAttributesToGet(UNLOCKED_ITEM, null);
        assertEquals(UNLOCKED_ITEM, result);
    }

    @Test
    public void filterAttributesToGetReturnsItemWhenAttributesToGetIsEmpty() {
        Map<String, AttributeValue> result = isolationHandler.filterAttributesToGet(UNLOCKED_ITEM, new ArrayList<String>());
        assertEquals(UNLOCKED_ITEM, result);
    }

    @Test
    public void filterAttributesToGetReturnsItemWhenAttributesToGetContainsAllAttributes() {
        List<String> attributesToGet = Arrays.asList("Id", "attr1"); // all attributes
        Map<String, AttributeValue> result = isolationHandler.filterAttributesToGet(UNLOCKED_ITEM, attributesToGet);
        assertEquals(UNLOCKED_ITEM, result);
    }

    @Test
    public void filterAttributesToGetReturnsOnlySpecifiedAttributesWhenSpecified() {
        List<String> attributesToGet = Arrays.asList("Id"); // only keep the key
        Map<String, AttributeValue> result = isolationHandler.filterAttributesToGet(UNLOCKED_ITEM, attributesToGet);
        assertEquals(KEY, result);
    }

    @Test(expected = TransactionAssertionException.class)
    public void getOldCommittedItemThrowsExceptionIfNoLockingRequestExists() {
        when(mockTxItem.getRequestForKey(TABLE_NAME, KEY)).thenReturn(null);
        isolationHandler.getOldCommittedItem(mockTx, TABLE_NAME, KEY);
    }

    @Test(expected = UnknownCompletedTransactionException.class)
    public void getOldCommittedItemThrowsExceptionIfOldItemDoesNotExist() {
        when(mockTxItem.getRequestForKey(TABLE_NAME, KEY)).thenReturn(mockRequest);
        when(mockRequest.getRid()).thenReturn(RID);
        when(mockTxItem.loadItemImage(RID)).thenReturn(null);
        isolationHandler.getOldCommittedItem(mockTx, TABLE_NAME, KEY);
    }

    @Test
    public void getOldCommittedItemReturnsOldImageIfOldItemExists() {
        when(mockTxItem.getRequestForKey(TABLE_NAME, KEY)).thenReturn(mockRequest);
        when(mockRequest.getRid()).thenReturn(RID);
        when(mockTxItem.loadItemImage(RID)).thenReturn(UNLOCKED_ITEM);
        Map<String, AttributeValue> result = isolationHandler.getOldCommittedItem(mockTx, TABLE_NAME, KEY);
        assertEquals(UNLOCKED_ITEM, result);
    }

    @Test
    public void createGetItemRequestCorrectlyCreatesRequest() {
        when(mockTxManager.createKeyMap(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM)).thenReturn(KEY);
        GetItemRequest request = isolationHandler.createGetItemRequest(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM);
        assertEquals(TABLE_NAME, request.getTableName());
        assertEquals(KEY, request.getKey());
        assertEquals(null, request.getAttributesToGet());
        assertTrue(request.getConsistentRead());
    }

    @Test
    public void handleItemReturnsNullForNullItem() {
        assertNull(isolationHandler.handleItem(null, TABLE_NAME, 0));
    }

    @Test
    public void handleItemReturnsItemForUnlockedItem() {
        assertEquals(UNLOCKED_ITEM, isolationHandler.handleItem(UNLOCKED_ITEM, TABLE_NAME, 0));
    }

    @Test
    public void handleItemReturnsNullForTransientItem() {
        assertNull(isolationHandler.handleItem(TRANSIENT_APPLIED_ITEM, TABLE_NAME, 0));
        assertNull(isolationHandler.handleItem(TRANSIENT_UNAPPLIED_ITEM, TABLE_NAME, 0));
    }

    @Test(expected = TransactionException.class)
    public void handleItemThrowsExceptionForNonTransientAppliedItemWithNoCorrespondingTx() {
        doThrow(TransactionNotFoundException.class).when(isolationHandler).loadTransaction(TX_ID);
        isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, TABLE_NAME, 0);
    }

    @Test
    public void handleItemReturnsItemForNonTransientAppliedItemWithCommittedTxItem() {
        doReturn(mockTx).when(isolationHandler).loadTransaction(TX_ID);
        when(mockTxItem.getState()).thenReturn(State.COMMITTED);
        assertEquals(NON_TRANSIENT_APPLIED_ITEM, isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, TABLE_NAME, 0));
    }

    @Test
    public void handleItemReturnsOldVersionOfItemForNonTransientAppliedItemWithPendingTxItem() {
        doReturn(mockTx).when(isolationHandler).loadTransaction(TX_ID);
        doReturn(UNLOCKED_ITEM).when(isolationHandler).getOldCommittedItem(mockTx, TABLE_NAME, KEY);
        when(mockTxManager.createKeyMap(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM)).thenReturn(KEY);
        when(mockTxItem.getState()).thenReturn(State.PENDING);
        when(mockTxItem.getRequestForKey(TABLE_NAME, KEY)).thenReturn(mockRequest);
        assertEquals(UNLOCKED_ITEM, isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, TABLE_NAME, 0));
        verify(isolationHandler).loadTransaction(TX_ID);
    }

    @Test(expected = TransactionException.class)
    public void handleItemThrowsExceptionForNonTransientAppliedItemWithPendingTxItemWithNoOldVersionAndNoRetries() {
        doReturn(mockTx).when(isolationHandler).loadTransaction(TX_ID);
        doThrow(UnknownCompletedTransactionException.class).when(isolationHandler).getOldCommittedItem(mockTx, TABLE_NAME, KEY);
        when(mockTxItem.getState()).thenReturn(State.PENDING);
        when(mockTxItem.getRequestForKey(TABLE_NAME, KEY)).thenReturn(mockRequest);
        isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, TABLE_NAME, 0);
        verify(isolationHandler).loadTransaction(TX_ID);
    }

    @Test
    public void handleItemRetriesWhenTransactionNotFound() {
        doThrow(TransactionNotFoundException.class).when(isolationHandler).loadTransaction(TX_ID);
        when(mockTxManager.createKeyMap(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM)).thenReturn(KEY);
        when(mockClient.getItem(GET_ITEM_REQUEST)).thenReturn(new GetItemResult().withItem(NON_TRANSIENT_APPLIED_ITEM));
        boolean caughtException = false;
        try {
            isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, TABLE_NAME, 1);
        } catch (TransactionException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
        verify(isolationHandler, times(2)).loadTransaction(TX_ID);
        verify(isolationHandler).createGetItemRequest(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM);
        verify(mockClient).getItem(GET_ITEM_REQUEST);
    }

    @Test
    public void handleItemRetriesWhenUnknownCompletedTransaction() {
        doReturn(mockTx).when(isolationHandler).loadTransaction(TX_ID);
        doThrow(UnknownCompletedTransactionException.class).when(isolationHandler).getOldCommittedItem(mockTx, TABLE_NAME, KEY);
        when(mockTxManager.createKeyMap(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM)).thenReturn(KEY);
        when(mockClient.getItem(GET_ITEM_REQUEST)).thenReturn(new GetItemResult().withItem(NON_TRANSIENT_APPLIED_ITEM));
        boolean caughtException = false;
        try {
            isolationHandler.handleItem(NON_TRANSIENT_APPLIED_ITEM, TABLE_NAME, 1);
        } catch (TransactionException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
        verify(isolationHandler, times(2)).loadTransaction(TX_ID);
        verify(isolationHandler).createGetItemRequest(TABLE_NAME, NON_TRANSIENT_APPLIED_ITEM);
        verify(mockClient).getItem(GET_ITEM_REQUEST);
    }
}
