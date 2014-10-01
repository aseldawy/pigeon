/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.pigeon;

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;

import com.esri.core.geometry.ogc.OGCGeometry;

public class TestGeometryParser extends TestCase {
  
  private ESRIGeometryParser geometry_parser = new ESRIGeometryParser();
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
      assertTrue(hex.equals(ESRIGeometryParser.bytesToHex(binary)));
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
      assertTrue(Arrays.equals(binary, ESRIGeometryParser.hexToBytes(hex)));
    }
  }

  public void testShouldParseWKT() throws Exception {
    String wkt = polygon.asText();
    OGCGeometry parsed = geometry_parser.parseGeom(wkt);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldParseHexString() throws Exception {
    byte[] binary = polygon.asBinary().array();
    String hex = ESRIGeometryParser.bytesToHex(binary);
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
