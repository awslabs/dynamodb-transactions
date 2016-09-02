/**
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
