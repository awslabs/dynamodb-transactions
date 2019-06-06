/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.transactions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
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
    
    // Any requests added to this set will throw a FailedYourRequestException when called.
    public final Set<AmazonWebServiceRequest> requestsToFail = new HashSet<AmazonWebServiceRequest>();
    
    // Any requests added to this set will return a null item when called
    public final Set<GetItemRequest> getRequestsToTreatAsDeleted = new HashSet<GetItemRequest>();
    
    // Any requests with keys in this set will return the queue of responses in order. When the end of the queue is reached
    // further requests will be passed to the DynamoDB client.
    public final Map<GetItemRequest, Queue<GetItemResult>> getRequestsToStub = new HashMap<GetItemRequest, Queue<GetItemResult>>(); 
    
    /**
     * Resets the client to the stock DynamoDB client (all requests will call DynamoDB)
     */
    public void reset() {
        requestsToFail.clear();
        getRequestsToTreatAsDeleted.clear();
        getRequestsToStub.clear();
    }
    
    public FailingAmazonDynamoDBClient(AWSCredentials credentials) {
        super(credentials);
    }
    
    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) throws AmazonServiceException, AmazonClientException {
        if(requestsToFail.contains(getItemRequest)) {
            throw new FailedYourRequestException();
        }
        if (getRequestsToTreatAsDeleted.contains(getItemRequest)) {
            return new GetItemResult();
        }
        Queue<GetItemResult> stubbedResults = getRequestsToStub.get(getItemRequest);
        if (stubbedResults != null && !stubbedResults.isEmpty()) {
            return stubbedResults.remove();
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
