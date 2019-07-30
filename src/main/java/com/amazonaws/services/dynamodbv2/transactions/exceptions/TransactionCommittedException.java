/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

/**
 * Thrown when a transaction was attempted to be rolled back, but it actually completed.
 */
public class TransactionCommittedException extends TransactionCompletedException {

    private static final long serialVersionUID = 1628959201410733660L;

    public TransactionCommittedException(String txId, String message) {
        super(txId, message);
    }
}
