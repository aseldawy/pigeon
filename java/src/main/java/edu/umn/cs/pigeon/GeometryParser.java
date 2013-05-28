/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package edu.umn.cs.pigeon;

import java.nio.ByteBuffer;

import org.apache.pig.data.DataByteArray;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.vividsolutions.jts.io.ParseException;

/**
 * Retrieves a geometry from a pig attribute. It detects the type of the column
 * and the data stored in that column and automatically detects its format
 * and tries to get the geometry object from it. In particular, here are the
 * checks done in order:
 * 1- If the object is of type bytearray, it is parsed as a well known binary
 *    (WKB). If the parsing fails with a parse exception, the binary array
 *    is converted to a string and the next step is carried out.
 * 2- If the object is of type chararrray or step 1 fails, the string is parsed
 *    as a well known text (WKT). If the parsing fails and the string contains
 *    only hex characters (0-9 and A-Z), it is converted to binary and parsed
 *    as a well known binary (WKB).
 * @author Ahmed Eldawy
 *
 */
public class GeometryParser {
  
  public OGCGeometry parseGeom(Object o) throws ParseException {
    OGCGeometry geom = null;
    if (o instanceof DataByteArray) {
      try {
        // Parse data as well known binary (WKB)
        byte[] bytes = ((DataByteArray) o).get();
        geom = OGCGeometry.fromBinary(ByteBuffer.wrap(bytes));
      } catch (RuntimeException e) {
        // Treat it as an encoded string (WKT)
        o = new String(((DataByteArray) o).get());
      }
    }
    if (o instanceof String) {
      try {
        // Parse string as well known text (WKT)
        geom = OGCGeometry.fromText((String) o);
      } catch (IllegalArgumentException e) {
        try {
          // Error parsing from WKT, try hex string instead
          byte[] binary = hexToBytes((String) o);
          geom = OGCGeometry.fromBinary(ByteBuffer.wrap(binary));
        } catch (RuntimeException e1) {
          // Cannot parse text. Just return null
        }
      }
    }
    return geom;
  }
  
  public static double parseDouble(Object o) {
    if (o instanceof Integer)
      return (Integer)o;
    if (o instanceof Double)
      return (Double)o;
    if (o instanceof DataByteArray)
      return Double.parseDouble(new String(((DataByteArray) o).get()));
    throw new RuntimeException("Cannot parse "+o+" into double");
  }

  private static final byte[] HexLookupTable = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
    'F'
  };
  
  /**
   * Convert binary array to a hex string.
   * @param binary
   * @return
   */
  public static String bytesToHex(byte[] binary) {
    // Each byte is converted to two hex values
    byte[] hex = new byte[binary.length * 2];
    for (int i = 0; i < binary.length; i++) {
      hex[2*i] = HexLookupTable[(binary[i] & 0xFF) >>> 4];
      hex[2*i+1] = HexLookupTable[binary[i] & 0xF];
    }
    return new String(hex);
  }
  
  /**
   * Convert a string containing a hex string to a byte array of binary.
   * For example, the string "AABB" is converted to the byte array {0xAA, 0XBB}
   * @param hex
   * @return
   */
  public static byte[] hexToBytes(String hex) {
    byte[] bytes = new byte[(hex.length() + 1) / 2];
    for (int i = 0; i < hex.length(); i++) {
      byte x = (byte) hex.charAt(i);
      if (x >= '0' && x <= '9')
        x -= '0';
      else if (x >= 'a' && x <= 'f')
        x = (byte) ((x - 'a') + 0xa);
      else if (x >= 'A' && x <= 'F')
        x = (byte) ((x - 'A') + 0xA);
      else
        throw new RuntimeException("Invalid hex char "+x);
      if (i % 2 == 0)
        x <<= 4;
      bytes[i / 2] |= x;
    }
    return bytes;
  }
}
