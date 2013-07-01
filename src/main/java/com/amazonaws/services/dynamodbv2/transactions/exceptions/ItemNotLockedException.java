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
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Indicates that the transaction could not get the lock because it is owned by another transaction.
 */
public class ItemNotLockedException extends TransactionException {

    private static final long serialVersionUID = -2992047273290608776L;

    private final String txId;
    private final String lockOwnerTxId;
    private final String tableName;
    private final Map<String, AttributeValue> item;

    public ItemNotLockedException(String txId, String lockTxId, String tableName, Map<String, AttributeValue> item) {
        this(txId, lockTxId, tableName, item, null);
    }
    
    public ItemNotLockedException(String txId, String lockOwnerTxId, String tableName, Map<String, AttributeValue> item, Throwable t) {
        super(txId, "Item is not locked by our transaction, is locked by " + lockOwnerTxId + " for table " + tableName + ", item: "+ item);
        this.txId = txId;
        this.lockOwnerTxId = lockOwnerTxId;
        this.tableName = tableName;
        this.item = item;
    }
    
    public String getTxId() {
        return txId;
    }

    public String getLockOwnerTxId() {
        return lockOwnerTxId;
    }

    public Map<String, AttributeValue> getItem() {
        return item;
    }

    public String getTableName() {
        return tableName;
    }

}
