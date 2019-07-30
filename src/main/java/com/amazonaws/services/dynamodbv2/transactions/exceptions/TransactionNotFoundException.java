/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;


/**
 * Indicates that the transaction record no longer exists (or never did)
 */
public class TransactionNotFoundException extends TransactionException {

    private static final long serialVersionUID = 1482803351154923519L;

    public TransactionNotFoundException(String txId) {
        super(txId, "Transaction not found");
    }
}
