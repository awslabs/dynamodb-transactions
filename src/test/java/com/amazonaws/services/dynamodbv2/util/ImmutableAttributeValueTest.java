/**
 * Copyright 2013-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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
