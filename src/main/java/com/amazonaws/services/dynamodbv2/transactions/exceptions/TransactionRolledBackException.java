/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

/**
 * Thrown when a transaction was attempted to be committed, but it actually rolled back.
 */
public class TransactionRolledBackException extends TransactionCompletedException {

    private static final long serialVersionUID = 1628959201410733660L;

    public TransactionRolledBackException(String txId, String message) {
        super(txId, message);
    }
}
