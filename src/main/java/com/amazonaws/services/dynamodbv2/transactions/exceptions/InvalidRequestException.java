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
