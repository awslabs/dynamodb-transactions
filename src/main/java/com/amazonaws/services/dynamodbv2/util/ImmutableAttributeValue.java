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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * An immutable class that can be used in map keys.  Does a copy of the attribute value
 * to prevent any member from being mutated.
 */
public class ImmutableAttributeValue {

    private final String n;
    private final String s;
    private final byte[] b;
    private final List<String> ns;
    private final List<String> ss;
    private final List<byte[]> bs;
    
    public ImmutableAttributeValue(AttributeValue av) {
        s = av.getS();
        n = av.getN();
        b = av.getB() != null ? av.getB().array().clone() : null;
        ns = av.getNS() != null ? new ArrayList<String>(av.getNS()) : null;
        ss = av.getSS() != null ? new ArrayList<String>(av.getSS()) : null;
        bs = av.getBS() != null ? new ArrayList<byte[]>(av.getBS().size()) : null;
        
        if(av.getBS() != null) {
            for(ByteBuffer buf : av.getBS()) {
                if(buf != null) {
                    bs.add(buf.array().clone());
                } else {
                    bs.add(null);
                }
            }
        }
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(b);
        result = prime * result + ((bs == null) ? 0 : bs.hashCode());
        result = prime * result + ((n == null) ? 0 : n.hashCode());
        result = prime * result + ((ns == null) ? 0 : ns.hashCode());
        result = prime * result + ((s == null) ? 0 : s.hashCode());
        result = prime * result + ((ss == null) ? 0 : ss.hashCode());
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
        ImmutableAttributeValue other = (ImmutableAttributeValue) obj;
        if (!Arrays.equals(b, other.b))
            return false;
        if (bs == null) {
            if (other.bs != null)
                return false;
        }
        else if (!bs.equals(other.bs)) {
            // Note: this else if block is not auto-generated
            if(other.bs == null)
                return false;
            if(bs.size() != other.bs.size())
                return false;
            for(int i = 0; i < bs.size(); i++) {
                if (!Arrays.equals(bs.get(i), other.bs.get(i)))
                    return false;
            }
            return true;
        }
        if (n == null) {
            if (other.n != null)
                return false;
        }
        else if (!n.equals(other.n))
            return false;
        if (ns == null) {
            if (other.ns != null)
                return false;
        }
        else if (!ns.equals(other.ns))
            return false;
        if (s == null) {
            if (other.s != null)
                return false;
        }
        else if (!s.equals(other.s))
            return false;
        if (ss == null) {
            if (other.ss != null)
                return false;
        }
        else if (!ss.equals(other.ss))
            return false;
        return true;
    }
    
}
