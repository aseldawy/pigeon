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

package edu.umn.cs.pigeon.test;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.io.WKBWriter;

import edu.umn.cs.pigeon.GeometryParser;

public class TestGeometryParser extends TestCase {
  
  private GeometryParser geometry_parser = new GeometryParser();
  private Geometry polygon;
  
  public TestGeometryParser() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(0, 0);
    coordinates[1] = new Coordinate(0, 3);
    coordinates[2] = new Coordinate(4, 5);
    coordinates[3] = new Coordinate(10, 0);
    coordinates[4] = new Coordinate(0, 0);
    LinearRing line = geometryFactory.createLinearRing(coordinates);
    polygon = geometryFactory.createPolygon(line, null);
  }

  public void testShouldParseWKT() throws Exception {
    String wkt = polygon.toString();
    Geometry parsed = geometry_parser.parseGeom(wkt);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldParseHexString() throws Exception {
    byte[] binary = new WKBWriter().write(polygon);
    String hex = WKBWriter.bytesToHex(binary);
    Geometry parsed = geometry_parser.parseGeom(hex);
    assertTrue(polygon.equals(parsed));
  }
  
  public void testShouldParseWKB() throws Exception {
    byte[] binary = new WKBWriter().write(polygon);
    DataByteArray barray = new DataByteArray(binary);
    Geometry parsed = geometry_parser.parseGeom(barray);
    assertTrue(polygon.equals(parsed));
  }
  
  public void testShouldParseWKTEncodedInBinary() throws Exception {
    String wkt = polygon.toString();
    DataByteArray barray = new DataByteArray(wkt);
    Geometry parsed = geometry_parser.parseGeom(barray);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldReturnNullOnGarbageText() throws Exception {
    Geometry parsed = geometry_parser.parseGeom("asdfasdf");
    assertNull(parsed);
  }

  public void testShouldReturnNullOnGarbageBinary() throws Exception {
    Geometry parsed = geometry_parser.parseGeom(new DataByteArray(new byte[] {0, 1, 2, 3}));
    assertNull(parsed);
  }
}
