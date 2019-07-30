/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

/**
 * Thrown when a transaction is completed (either committed or rolled back) and it wasn't the expectation of the caller
 * for this to happen .
 */
public class TransactionCompletedException extends TransactionException {

    private static final long serialVersionUID = -8170993155989412979L;

    public TransactionCompletedException(String txId, String message) {
        super(txId, message);
    }
}
