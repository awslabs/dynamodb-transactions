/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
 package com.amazonaws.services.dynamodbv2.transactions.exceptions;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.transactions.Request;

public class InvalidRequestException extends TransactionException {
    
    private static final long serialVersionUID = 4622315126910271817L;

    private final String tableName;
    private final Map<String, AttributeValue> key;
    private final Request request;
    
    public InvalidRequestException(String message, String txId, String tableName, Map<String, AttributeValue> key, Request request) {
        this(message, txId, tableName, key, request, null);
    }
    
    public InvalidRequestException(String message, String txId, String tableName, Map<String, AttributeValue> key, Request request, Throwable t) {
        super(((message != null) ? ": " + message : "Invalid request") + " for transaction " + txId + " table " + tableName + " key " + key, t);
        this.tableName = tableName;
        this.key = key;
        this.request = request;
    }
    
    public String getTableName() {
        return tableName;
    }

    public Map<String, AttributeValue> getKey() {
        return key;
    }

    public Request getRequest() {
        return request;
    }
    
}
