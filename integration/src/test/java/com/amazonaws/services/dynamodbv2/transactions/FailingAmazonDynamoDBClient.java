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
package com.amazonaws.services.dynamodbv2.transactions;

import java.util.HashSet;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 * A very primitive fault-injection client.
 *
 * @author dyanacek
 */
public class FailingAmazonDynamoDBClient extends AmazonDynamoDBClient {

    public static class FailedYourRequestException extends RuntimeException {
        private static final long serialVersionUID = -7191808024168281212L;
    }
    
    public final Set<AmazonWebServiceRequest> requestsToFail = new HashSet<AmazonWebServiceRequest>();
    
    public FailingAmazonDynamoDBClient(AWSCredentials credentials) {
        super(credentials);
    }
    
    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) throws AmazonServiceException, AmazonClientException {
        if(requestsToFail.contains(getItemRequest)) {
            throw new FailedYourRequestException();
        }
        return super.getItem(getItemRequest);
    }
    
    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) throws AmazonServiceException,
        AmazonClientException {
        if(requestsToFail.contains(updateItemRequest)) {
            throw new FailedYourRequestException();
        }
        return super.updateItem(updateItemRequest);
    }
}
