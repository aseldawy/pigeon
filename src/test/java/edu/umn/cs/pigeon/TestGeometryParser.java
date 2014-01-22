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

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;

import com.esri.core.geometry.ogc.OGCGeometry;

public class TestGeometryParser extends TestCase {
  
  private GeometryParser geometry_parser = new GeometryParser();
  private OGCGeometry polygon;
  
  public TestGeometryParser() {
    polygon = OGCGeometry.fromText("Polygon ((0 0, 0 3, 4 5, 10 0, 0 0))");
  }
  
  public void testShouldConvertBinaryToHex() throws Exception {
    byte[][] binaryTable = new byte[][] {
        {(byte)0xaa, (byte)0xbb, (byte)0xcd, 0x0f},
        {}
    };
    String[] hexTable = {
      "AABBCD0F",
      ""
    };
    for (int i = 0; i < binaryTable.length; i ++) {
      byte[] binary = binaryTable[i];
      String hex = hexTable[i];
      assertTrue(hex.equals(GeometryParser.bytesToHex(binary)));
    }
  }
  
  public void testShouldConvertHexToBinary() throws Exception {
    byte[][] binaryTable = new byte[][] {
        {(byte)0xaa, (byte)0xbb, (byte)0xcd, 0x0f},
        {}
    };
    String[] hexTable = {
      "AABBCD0F",
      ""
    };
    for (int i = 0; i < binaryTable.length; i ++) {
      byte[] binary = binaryTable[i];
      String hex = hexTable[i];
      assertTrue(Arrays.equals(binary, GeometryParser.hexToBytes(hex)));
    }
  }

  public void testShouldParseWKT() throws Exception {
    String wkt = polygon.asText();
    OGCGeometry parsed = geometry_parser.parseGeom(wkt);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldParseHexString() throws Exception {
    byte[] binary = polygon.asBinary().array();
    String hex = GeometryParser.bytesToHex(binary);
    OGCGeometry parsed = geometry_parser.parseGeom(hex);
    assertTrue(polygon.equals(parsed));
  }
  
  public void testShouldParseWKB() throws Exception {
    byte[] binary = polygon.asBinary().array();
    DataByteArray barray = new DataByteArray(binary);
    OGCGeometry parsed = geometry_parser.parseGeom(barray);
    assertTrue(polygon.equals(parsed));
  }
  
  public void testShouldParseWKTEncodedInBinary() throws Exception {
    String wkt = polygon.asText();
    DataByteArray barray = new DataByteArray(wkt);
    OGCGeometry parsed = geometry_parser.parseGeom(barray);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldReturnNullOnGarbageText() throws Exception {
    OGCGeometry parsed = geometry_parser.parseGeom("asdfasdf");
    assertNull(parsed);
  }

  public void testShouldReturnNullOnGarbageBinary() throws Exception {
    OGCGeometry parsed = geometry_parser.parseGeom(new DataByteArray(new byte[] {0, 1, 2, 3}));
    assertNull(parsed);
  }
}
