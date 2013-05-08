package edu.umn.cs.spig.test;


import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.io.WKBWriter;

import edu.umn.cs.spig.GeometryParser;

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
    Geometry parsed = geometry_parser.parse(wkt);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldParseHexString() throws Exception {
    byte[] binary = new WKBWriter().write(polygon);
    String hex = WKBWriter.bytesToHex(binary);
    Geometry parsed = geometry_parser.parse(hex);
    assertTrue(polygon.equals(parsed));
  }
  
  public void testShouldParseWKB() throws Exception {
    byte[] binary = new WKBWriter().write(polygon);
    DataByteArray barray = new DataByteArray(binary);
    Geometry parsed = geometry_parser.parse(barray);
    assertTrue(polygon.equals(parsed));
  }
  
  public void testShouldParseWKTEncodedInBinary() throws Exception {
    String wkt = polygon.toString();
    DataByteArray barray = new DataByteArray(wkt);
    Geometry parsed = geometry_parser.parse(barray);
    assertTrue(polygon.equals(parsed));
  }

  public void testShouldReturnNullOnGarbageText() throws Exception {
    Geometry parsed = geometry_parser.parse("asdfasdf");
    assertNull(parsed);
  }

  public void testShouldReturnNullOnGarbageBinary() throws Exception {
    Geometry parsed = geometry_parser.parse(new DataByteArray(new byte[] {0, 1, 2, 3}));
    assertNull(parsed);
  }
}
