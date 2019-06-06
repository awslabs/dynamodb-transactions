/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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
