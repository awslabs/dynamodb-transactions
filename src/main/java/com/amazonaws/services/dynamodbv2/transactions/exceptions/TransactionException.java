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
