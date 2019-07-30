/**
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.transactions;


import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.List;
import java.util.Map;

/**
 * An isolation handler takes an item and returns a version
 * of the item that can be read at the implemented isolaction
 * level.
 */
public interface ReadIsolationHandler {

    /**
     * Returns a version of the item can be read at the isolation level implemented by
     * the handler. This is possibly null if the item is transient. It might not be latest
     * version if the isolation level is committed.
     * @param item The item to check
     * @param attributesToGet The attributes to get from the table. If null or empty, will
     *                        fetch all attributes.
     * @param tableName The table that contains the item
     * @return A version of the item that can be read at the isolation level.
     */
    public Map<String, AttributeValue> handleItem(
            final Map<String, AttributeValue> item,
            final List<String> attributesToGet,
            final String tableName);

}
