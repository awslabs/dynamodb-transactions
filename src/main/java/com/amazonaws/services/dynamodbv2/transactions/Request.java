/**
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import com.amazonaws.RequestClientOptions;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnItemCollectionMetrics;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.InvalidRequestException;
import com.amazonaws.services.dynamodbv2.transactions.exceptions.TransactionAssertionException;

/**
 * Represents a write or lock request within a transaction - either a PutItem, UpdateItem, DeleteItem, or a LockItem request used for read locks
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = Request.PutItem.class, name = "PutItem"),
    @Type(value = Request.UpdateItem.class, name = "UpdateItem"),
    @Type(value = Request.DeleteItem.class, name = "DeleteItem"),
    @Type(value = Request.GetItem.class, name = "GetItem")
})
public abstract class Request {
    
    private static final Set<String> VALID_RETURN_VALUES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("ALL_OLD", "ALL_NEW", "NONE")));
    
    private Integer rid;

    @JsonIgnore
    protected abstract String getTableName();
    
    @JsonIgnore
    protected abstract Map<String, AttributeValue> getKey(TransactionManager txManager);

    @JsonIgnore
    protected abstract String getReturnValues();
    
    @JsonIgnore
    protected abstract void doValidate(String txId, TransactionManager txManager);
    
    /*
     * Request implementations
     * 
     */
    
    public Integer getRid() {
        return rid;
    }
    
    public void setRid(Integer rid) {
        this.rid = rid;
    }
    
    @JsonTypeName( value="GetItem" )
    public static class GetItem extends Request {
        
        private GetItemRequest request;
        
        public GetItemRequest getRequest() {
            return request;
        }
        
        public void setRequest(GetItemRequest request) {
            this.request = request;
        }
        
        @Override
        protected String getTableName() {
            return request.getTableName();
        }
        
        @Override
        protected String getReturnValues() {
            return null;
        }
        
        @Override
        protected Map<String, AttributeValue> getKey(TransactionManager txManager) {
            return request.getKey();
        }
        
        @Override
        protected void doValidate(String txId, TransactionManager txManager) {
            validateAttributes(this, request.getKey(), txId, txManager);
            validateAttributes(this, request.getAttributesToGet(), txId, txManager);
        }
    }
    
    @JsonTypeName( value="UpdateItem" )
    public static class UpdateItem extends Request {
        
        private UpdateItemRequest request;
        
        public UpdateItemRequest getRequest() {
            return request;
        }
        
        public void setRequest(UpdateItemRequest request) {
            this.request = request;
        }
        
        @Override
        protected String getTableName() {
            return request.getTableName();
        }
        
        @Override
        protected String getReturnValues() {
            return request.getReturnValues();
        }
        
        @Override
        protected Map<String, AttributeValue> getKey(TransactionManager txManager) {
            return request.getKey();
        }
        
        @Override
        protected void doValidate(String txId, TransactionManager txManager) {
            validateAttributes(this, request.getKey(), txId, txManager);
            if(request.getAttributeUpdates() != null) {
                validateAttributes(this, request.getAttributeUpdates(), txId, txManager);
            }
            if(request.getReturnConsumedCapacity() != null) {
                throw new InvalidRequestException("ReturnConsumedCapacity is not currently supported", txId, request.getTableName(), null, this);
            }
            if(request.getReturnItemCollectionMetrics() != null) {
                throw new InvalidRequestException("ReturnItemCollectionMetrics is not currently supported", txId, request.getTableName(), null, this);
            }
            if(request.getExpected() != null) {
                throw new InvalidRequestException("Requests with conditions are not currently supported", txId, request.getTableName(), getKey(txManager), this);
            }
            if(request.getConditionExpression() != null) {
                throw new InvalidRequestException("Requests with conditions are not currently supported", txId, request.getTableName(), getKey(txManager), this);
            }
            if(request.getUpdateExpression() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, request.getTableName(), getKey(txManager), this);
            }
            if(request.getExpressionAttributeNames() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getExpressionAttributeValues() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
        }
    }
    
    @JsonTypeName( value="DeleteItem" )
    public static class DeleteItem extends Request {
        
        private DeleteItemRequest request;
        
        public DeleteItemRequest getRequest() {
            return request;
        }
        
        public void setRequest(DeleteItemRequest request) {
            this.request = request;
        }
        
        @Override
        protected String getTableName() {
            return request.getTableName();
        }
        
        @Override
        protected String getReturnValues() {
            return request.getReturnValues();
        }
        
        @Override
        protected Map<String, AttributeValue> getKey(TransactionManager txManager) {
            return request.getKey();
        }
        
        @Override
        protected void doValidate(String txId, TransactionManager txManager) {
            validateAttributes(this, request.getKey(), txId, txManager);
            if(request.getReturnConsumedCapacity() != null) {
                throw new InvalidRequestException("ReturnConsumedCapacity is not currently supported", txId, getTableName(), null, this);
            }
            if(request.getReturnItemCollectionMetrics() != null) {
                throw new InvalidRequestException("ReturnItemCollectionMetrics is not currently supported", txId, getTableName(), null, this);
            }
            if(request.getExpected() != null) {
                throw new InvalidRequestException("Requests with conditions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getConditionExpression() != null) {
                throw new InvalidRequestException("Requests with conditions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getExpressionAttributeNames() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getExpressionAttributeValues() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
        }
    }
    
    @JsonTypeName( value="PutItem" )
    public static class PutItem extends Request {
        
        @JsonIgnore
        private Map<String, AttributeValue> key = null;
        
        private PutItemRequest request;
        
        public PutItemRequest getRequest() {
            return request;
        }
        
        public void setRequest(PutItemRequest request) {
            this.request = request;
        }
        
        @Override
        protected String getTableName() {
            return request.getTableName();
        }
        
        @Override
        protected String getReturnValues() {
            return request.getReturnValues();
        }
        
        @Override
        protected Map<String, AttributeValue> getKey(TransactionManager txManager) {
            if(key == null) {
                 key = getKeyFromItem(getTableName(), request.getItem(), txManager);
            }
            return key;
        }
        
        @Override
        protected void doValidate(String txId, TransactionManager txManager) {
            if(request == null || request.getItem() == null) {
                throw new InvalidRequestException("PutItem must contain an Item", txId, getTableName(), null, this);
            }
            validateAttributes(this, request.getItem(), txId, txManager);
            if(request.getReturnConsumedCapacity() != null) {
                throw new InvalidRequestException("ReturnConsumedCapacity is not currently supported", txId, getTableName(), null, this);
            }
            if(request.getReturnItemCollectionMetrics() != null) {
                throw new InvalidRequestException("ReturnItemCollectionMetrics is not currently supported", txId, getTableName(), null, this);
            }
            if(request.getExpected() != null) {
                throw new InvalidRequestException("Requests with conditions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getConditionExpression() != null) {
                throw new InvalidRequestException("Requests with conditions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getExpressionAttributeNames() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
            if(request.getExpressionAttributeValues() != null) {
                throw new InvalidRequestException("Requests with expressions are not currently supported", txId, getTableName(), getKey(txManager), this);
            }
        }
    }

    /*
     * Validation helpers
     */
    
    @JsonIgnore
    public void validate(String txId, TransactionManager txManager) {
        if(getTableName() == null) {
            throw new InvalidRequestException("TableName must not be null", txId, null, null, this);
        }
        Map<String, AttributeValue> key = getKey(txManager); 
        if(key == null || key.isEmpty()) {
            throw new InvalidRequestException("The request key cannot be empty", txId, getTableName(), key, this);
        }
        
        validateReturnValues(getReturnValues(), txId, this);
        
        doValidate(txId, txManager);
    }
    
    private static void validateReturnValues(String returnValues, String txId, Request request) {
        if(returnValues == null || VALID_RETURN_VALUES.contains(returnValues)) {
            return;
        }
        
        throw new InvalidRequestException("Unsupported ReturnValues: " + returnValues, txId, request.getTableName(), null, request);
    }
    
    private static void validateAttributes(Request request, Map<String, ?> attributes, String txId, TransactionManager txManager) {
        for(Map.Entry<String, ?> entry : attributes.entrySet()) {
            if(entry.getKey().startsWith("_Tx")) {
                throw new InvalidRequestException("Request must not contain the reserved attribute " + entry.getKey(), 
                    txId, request.getTableName(), request.getKey(txManager), request);
            }
        }
    }
    
    private static void validateAttributes(Request request, List<String> attributes, String txId, TransactionManager txManager) {
        if(attributes == null) {
            return;
        }
        for(String attr : attributes) {
            if(attr.startsWith("_Tx")) {
                throw new InvalidRequestException("Request must not contain the reserved attribute " + attr, 
                    txId, request.getTableName(), request.getKey(txManager), request);
            }
        }
    }
    
    protected static Map<String, AttributeValue> getKeyFromItem(String tableName, Map<String, AttributeValue> item, TransactionManager txManager) {
        if(item == null) {
            throw new InvalidRequestException("PutItem must contain an Item", null, tableName, null, null);
        }
        Map<String, AttributeValue> newKey = new HashMap<String, AttributeValue>();
        List<KeySchemaElement> schema = txManager.getTableSchema(tableName);
        for(KeySchemaElement schemaElement : schema) {
            AttributeValue val = item.get(schemaElement.getAttributeName());
            if(val == null) {
                throw new InvalidRequestException("PutItem request must contain the key attribute " + schemaElement.getAttributeName(), null, tableName, null, null);
            }
            newKey.put(schemaElement.getAttributeName(), item.get(schemaElement.getAttributeName()));
        }
        return newKey;
    }
    
    /**
     * Returns a new copy of Map that can be used in a write on the item to ensure it does not exist 
     * @param txManager
     * @return a map for use in an expected clause to ensure the item does not exist
     */
    @JsonIgnore
    protected Map<String, ExpectedAttributeValue> getExpectNotExists(TransactionManager txManager) {
        Map<String, AttributeValue> key = getKey(txManager);
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>(key.size());
        for(Map.Entry<String, AttributeValue> entry : key.entrySet()) {
            expected.put(entry.getKey(), new ExpectedAttributeValue().withExists(false));
        }
        return expected;
    }
    
    /**
     * Returns a new copy of Map that can be used in a write on the item to ensure it exists 
     * @param txManager
     * @return a map for use in an expected clause to ensure the item exists
     */
    @JsonIgnore
    protected Map<String, ExpectedAttributeValue> getExpectExists(TransactionManager txManager) {
        Map<String, AttributeValue> key = getKey(txManager);
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>(key.size());
        for(Map.Entry<String, AttributeValue> entry : key.entrySet()) {
            expected.put(entry.getKey(), new ExpectedAttributeValue().withValue(entry.getValue()));
        }
        return expected;
    }
    
    /*
     * Serialization configuration related code
     */
    
    private static final ObjectMapper MAPPER;
    
    static {
        MAPPER = new ObjectMapper();
        MAPPER.disable(SerializationFeature.INDENT_OUTPUT);
        MAPPER.setSerializationInclusion(Include.NON_NULL);
        MAPPER.addMixInAnnotations(GetItemRequest.class, RequestMixIn.class);
        MAPPER.addMixInAnnotations(PutItemRequest.class, RequestMixIn.class);
        MAPPER.addMixInAnnotations(UpdateItemRequest.class, RequestMixIn.class);
        MAPPER.addMixInAnnotations(DeleteItemRequest.class, RequestMixIn.class);
        MAPPER.addMixInAnnotations(AttributeValueUpdate.class, AttributeValueUpdateMixIn.class);
        MAPPER.addMixInAnnotations(ExpectedAttributeValue.class, ExpectedAttributeValueMixIn.class);
        
        // Deal with serializing of byte[].
        SimpleModule module = new SimpleModule("custom", Version.unknownVersion());
        module.addSerializer(ByteBuffer.class, new ByteBufferSerializer());
        module.addDeserializer(ByteBuffer.class, new ByteBufferDeserializer());
        MAPPER.registerModule(module);
    };
    
    protected static ByteBuffer serialize(String txId, Object request) {
        try {
            byte[] requestBytes = MAPPER.writeValueAsBytes(request);
            return ByteBuffer.wrap(requestBytes);
        } catch (JsonGenerationException e) {
            throw new TransactionAssertionException(txId, "Failed to serialize request " + request + " " + e);
        } catch (JsonMappingException e) {
            throw new TransactionAssertionException(txId, "Failed to serialize request " + request + " " + e);
        } catch (IOException e) {
            throw new TransactionAssertionException(txId, "Failed to serialize request " + request + " " + e);
        }
    }
    
    
    
    protected static Request deserialize(String txId, ByteBuffer rawRequest) {
        byte[] requestBytes = rawRequest.array();
        try {
            return MAPPER.readValue(requestBytes, 0, requestBytes.length, Request.class);
        } catch (JsonParseException e) {
            throw new TransactionAssertionException(txId, "Failed to deserialize request " + rawRequest + " " + e);
        } catch (JsonMappingException e) {
            throw new TransactionAssertionException(txId, "Failed to deserialize request " + rawRequest + " " + e);
        } catch (IOException e) {
            throw new TransactionAssertionException(txId, "Failed to deserialize request " + rawRequest + " " + e);
        }
    }
    
    private static abstract class AmazonWebServiceRequestMixIn {

        @JsonIgnore
        public abstract String getDelegationToken();

        @JsonIgnore
        public abstract void setDelegationToken(String delegationToken);

        @JsonIgnore
        public abstract void setRequestCredentials(AWSCredentials credentials);

        @JsonIgnore
        public abstract AWSCredentials getRequestCredentials();

        @JsonIgnore
        public abstract Map<String, String> copyPrivateRequestParameters();

        @JsonIgnore
        public abstract RequestClientOptions getRequestClientOptions();
        
        @JsonIgnore
        public abstract ProgressListener getGeneralProgressListener();
        
        @JsonIgnore
        public abstract void setGeneralProgressListener(ProgressListener progressListener);
        
        @JsonIgnore
        public abstract int getReadLimit();
        
        @JsonIgnore
        public abstract Map<String, String> getCustomRequestHeaders();
        
    }
    
    private static abstract class RequestMixIn extends AmazonWebServiceRequestMixIn {
        
        @JsonIgnore
        public abstract String getDelegationToken();

        @JsonIgnore
        public abstract void setDelegationToken(String delegationToken);

        @JsonIgnore
        public abstract void setRequestCredentials(AWSCredentials credentials);

        @JsonIgnore
        public abstract AWSCredentials getRequestCredentials();

        @JsonIgnore
        public abstract Map<String, String> copyPrivateRequestParameters();

        @JsonIgnore
        public abstract RequestClientOptions getRequestClientOptions();
        
        @JsonIgnore
        public abstract void setReturnValues(ReturnValue returnValue);
        
        @JsonProperty
        public abstract void setReturnValues(String returnValue);
        
        @JsonIgnore
        public abstract void setReturnConsumedCapacity(ReturnConsumedCapacity returnConsumedCapacity);
        
        @JsonProperty
        public abstract void setReturnConsumedCapacity(String returnConsumedCapacity);
        
        @JsonIgnore
        public abstract void setReturnItemCollectionMetrics(ReturnItemCollectionMetrics returnItemCollectionMetrics);
        
        @JsonProperty
        public abstract void setReturnItemCollectionMetrics(String returnItemCollectionMetrics);
        
        @JsonIgnore
        public abstract boolean isConsistentRead();
        
        @JsonProperty
        public abstract boolean getConsistentRead();

        @JsonIgnore
        public abstract void setConditionalOperator(ConditionalOperator conditionalOperator);
        
        @JsonProperty
        public abstract void setConditionalOperator(String conditionalOperator);

    }
        
    private static abstract class AttributeValueUpdateMixIn {

        @JsonIgnore
        public abstract void setAction(AttributeAction attributeAction);
        
        @JsonProperty
        public abstract void setAction(String attributeAction);
        
    }

    private static abstract class ExpectedAttributeValueMixIn {

        @JsonIgnore
        public abstract void setComparisonOperator(ComparisonOperator comparisonOperator);
        
        @JsonProperty
        public abstract void setComparisonOperator(String comparisonOperator);

    }

    private static class ByteBufferSerializer extends JsonSerializer<ByteBuffer> {
        
        @Override
        public void serialize(ByteBuffer value, JsonGenerator jgen,
                SerializerProvider provider) throws IOException,
                JsonProcessingException {
            // value is never null, according to JsonSerializer contract
            jgen.writeBinary(value.array());
        }
        
    }
    
    private static class ByteBufferDeserializer extends JsonDeserializer<ByteBuffer> {
        
        @Override
        public ByteBuffer deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            // never called for null literal, according to JsonDeserializer contract
            return ByteBuffer.wrap(jp.getBinaryValue());
        }
        
    }
    
}