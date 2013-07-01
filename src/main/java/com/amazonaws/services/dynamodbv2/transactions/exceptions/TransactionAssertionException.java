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

public class TransactionAssertionException extends TransactionException {
    
    private static final long serialVersionUID = -894664265849460781L;

    public TransactionAssertionException(String txId, String message) {
        super(txId, message);
    }
    
    /**
     * Throws an assertion exception with a message constructed from to toString() of each data pair, if the assertion is false.
     * @param assertion
     * @param txId
     * @param message
     * @param data
     */
    public static void txAssert(boolean assertion, String txId, String message, Object... data) {
        if(! assertion) {
            if(data != null) {
                StringBuilder sb = new StringBuilder();
                for(Object d : data) {
                    sb.append(d);
                    sb.append(", ");
                }
                message = message + " - " + sb.toString();
            }
            
            throw new TransactionAssertionException(txId, message);
        }
    }
    
    public static void txFail(String txId, String message, Object... data) {
        txAssert(false, txId, message, data);
    }
}
