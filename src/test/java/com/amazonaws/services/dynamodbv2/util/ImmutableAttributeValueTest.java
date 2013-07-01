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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.util.ImmutableAttributeValue;

public class ImmutableAttributeValueTest {

    @Test
    public void testBSEquals() {
        byte[] b1 = { (byte)0x01 };
        byte[] b2 = { (byte)0x01 };
        AttributeValue av1 = new AttributeValue().withBS(ByteBuffer.wrap(b1));
        AttributeValue av2 = new AttributeValue().withBS(ByteBuffer.wrap(b2));
        ImmutableAttributeValue iav1 = new ImmutableAttributeValue(av1);
        ImmutableAttributeValue iav2 = new ImmutableAttributeValue(av2);
        assertEquals(iav1, iav2);
    }
    
    @Test
    public void testBSNotEq() {
        byte[] b1 = { (byte)0x01 };
        byte[] b2 = { (byte)0x02 };
        AttributeValue av1 = new AttributeValue().withBS(ByteBuffer.wrap(b1));
        AttributeValue av2 = new AttributeValue().withBS(ByteBuffer.wrap(b2));
        ImmutableAttributeValue iav1 = new ImmutableAttributeValue(av1);
        ImmutableAttributeValue iav2 = new ImmutableAttributeValue(av2);
        assertFalse(iav1.equals(iav2));
    }
    
    @Test
    public void testBSWithNull() {
        byte[] b1 = { (byte)0x01 };
        byte[] b2 = { (byte)0x01 };
        AttributeValue av1 = new AttributeValue().withBS(ByteBuffer.wrap(b1), ByteBuffer.wrap(b1));
        AttributeValue av2 = new AttributeValue().withBS(ByteBuffer.wrap(b2), null);
        ImmutableAttributeValue iav1 = new ImmutableAttributeValue(av1);
        ImmutableAttributeValue iav2 = new ImmutableAttributeValue(av2);
        assertFalse(iav1.equals(iav2));
    }
    
}