/**
 * 
 */
package edu.umn.cs.spig.test;

import junit.framework.TestCase;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

import edu.umn.cs.spig.ST_Area;


/**
 * @author eldawy
 *
 */
public class TestST_Area extends TestCase {
  
  private Geometry polygon;
  
  public TestST_Area() {
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

  
  public void testShouldWorkWithByteArrayWKT() throws Exception {
    Tuple tuple = TupleFactory.getInstance().newTuple(1);
    tuple.set(0, polygon.toString());
    Double area = new ST_Area().exec(tuple);
    assertEquals(polygon.getArea(), area);
  }

}
