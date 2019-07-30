/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

/**
 * Thrown when a transaction is no longer pending, but it is not known whether it committed or was rolled back.
 */
public class UnknownCompletedTransactionException extends TransactionCompletedException {
    
    private static final long serialVersionUID = 612575052603020091L;

    public UnknownCompletedTransactionException(String txId, String message) {
        super(txId, message);
    }
}
