/**
 * 
 */
package edu.umn.cs.spig.test;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.umn.cs.spig.ST_Area;


/**
 * @author eldawy
 *
 */
public class TestST_Area extends TestCase {
  
  public void testShouldWorkWithByteArrayWKT() throws Exception {
    Tuple tuple = TupleFactory.getInstance().newTuple(1);
    tuple.set(0, new DataByteArray("Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"));
    Double area = new ST_Area().exec(tuple);
    assertEquals(1.0, area);
  }

  public void testShouldWorkWithCharArrayWKT() throws Exception {
    Tuple tuple = TupleFactory.getInstance().newTuple(1);
    tuple.set(0, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))");
    Double area = new ST_Area().exec(tuple);
    assertEquals(1.0, area);
  }
}
