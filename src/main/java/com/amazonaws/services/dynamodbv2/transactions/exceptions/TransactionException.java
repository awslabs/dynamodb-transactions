/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

public class TransactionException extends RuntimeException {

    private static final long serialVersionUID = -3886636775903901771L;
    
    private final String txId;
    
    public TransactionException(String txId, String message) {
        super(txId + " - " + message);
        this.txId = txId;
    }
    
    public TransactionException(String txId, String message, Throwable t) {
        super(txId + " - " + message, t);
        this.txId = txId;
    }
    
    public TransactionException(String txId, Throwable t) {
        super(txId + " - " + ((t != null) ? t.getMessage() : ""), t);
        this.txId = txId;
    }

    public String getTxId() {
        return txId;
    }
}
