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
 package com.amazonaws.services.dynamodbv2.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * An immutable, write-only key map for storing DynamoDB a primary key value as a map key in other maps.  
 */
public class ImmutableKey {

    private final Map<String, ImmutableAttributeValue> key;
    
    public ImmutableKey(Map<String, AttributeValue> mutableKey) {
        if(mutableKey == null) {
            this.key = null;
        } else {
            Map<String, ImmutableAttributeValue> keyBuilder = new HashMap<String, ImmutableAttributeValue>(mutableKey.size());
            for(Map.Entry<String, AttributeValue> e : mutableKey.entrySet()) {
                keyBuilder.put(e.getKey(), new ImmutableAttributeValue(e.getValue()));
            }
            this.key = Collections.unmodifiableMap(keyBuilder);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ImmutableKey other = (ImmutableKey) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        }
        else if (!key.equals(other.key))
            return false;
        return true;
    }
}
