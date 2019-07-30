/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;


public class DuplicateRequestException extends TransactionException {

    private static final long serialVersionUID = 5461061207526371210L;

    public DuplicateRequestException(String txId, String tableName, String key) {
        super(txId, "Duplicate request for table name " + tableName + " for key " + key);
    }

}
