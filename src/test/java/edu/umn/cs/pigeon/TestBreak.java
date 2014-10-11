/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.pigeon;

import static org.apache.pig.ExecType.LOCAL;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;


/**
 * Breaks down a linestring or polygon into line segments each containing
 * two points only.
 * @author Ahmed Eldawy
 *
 */
public class TestBreak extends TestCase {
  
  public void testShouldWorkWithLinestring() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "LINESTRING (0 0, 0 1, 1 3)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, geom);\n" +
      "B = FOREACH A GENERATE geom_id, FLATTEN("+Break.class.getName()+"(geom));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Vector<String[]> expectedResults = new Vector<String[]>();
    expectedResults.add(new String[] {"0", "0", "0", "0", "0", "1"});
    expectedResults.add(new String[] {"0", "1", "0", "1", "1", "3"});
    Iterator<String[]> expected = expectedResults.iterator();
    int count = 0;
    while (it.hasNext() && expected.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String[] expectedResult = expected.next();
      if (tuple == null)
        break;
      assertEquals(Integer.parseInt(expectedResult[0]), tuple.get(0));
      assertEquals(Integer.parseInt(expectedResult[1]), tuple.get(1));
      assertEquals(Double.parseDouble(expectedResult[2]), tuple.get(2));
      assertEquals(Double.parseDouble(expectedResult[3]), tuple.get(3));
      assertEquals(Double.parseDouble(expectedResult[4]), tuple.get(4));
      assertEquals(Double.parseDouble(expectedResult[5]), tuple.get(5));
      count++;
    }
    assertEquals(expectedResults.size(), count);
  }

  public void testShouldWorkWithPolygon() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "POLYGON ((0 0, 0 1, 1 0, 0 0))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, geom);\n" +
        "B = FOREACH A GENERATE geom_id, FLATTEN("+Break.class.getName()+"(geom));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Vector<String[]> expectedResults = new Vector<String[]>();
    expectedResults.add(new String[] {"1", "0", "0", "0", "0", "1"});
    expectedResults.add(new String[] {"1", "1", "0", "1", "1", "0"});
    expectedResults.add(new String[] {"1", "2", "1", "0", "0", "0"});
    Iterator<String[]> expected = expectedResults.iterator();
    int count = 0;
    while (it.hasNext() && expected.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String[] expectedResult = expected.next();
      if (tuple == null)
        break;
      assertEquals(Integer.parseInt(expectedResult[0]), tuple.get(0));
      assertEquals(Integer.parseInt(expectedResult[1]), tuple.get(1));
      assertEquals(Double.parseDouble(expectedResult[2]), tuple.get(2));
      assertEquals(Double.parseDouble(expectedResult[3]), tuple.get(3));
      assertEquals(Double.parseDouble(expectedResult[4]), tuple.get(4));
      assertEquals(Double.parseDouble(expectedResult[5]), tuple.get(5));
      count++;
    }
    assertEquals(expectedResults.size(), count);
  }

  public void testShouldWorkWithGeometryCollection() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 0, 0 0)))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, geom);\n" +
        "B = FOREACH A GENERATE geom_id, FLATTEN("+Break.class.getName()+"(geom));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Vector<String[]> expectedResults = new Vector<String[]>();
    expectedResults.add(new String[] {"1", "0", "0", "0", "0", "1"});
    expectedResults.add(new String[] {"1", "1", "0", "1", "1", "0"});
    expectedResults.add(new String[] {"1", "2", "1", "0", "0", "0"});
    Iterator<String[]> expected = expectedResults.iterator();
    int count = 0;
    while (it.hasNext() && expected.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String[] expectedResult = expected.next();
      if (tuple == null)
        break;
      assertEquals(Integer.parseInt(expectedResult[0]), tuple.get(0));
      assertEquals(Integer.parseInt(expectedResult[1]), tuple.get(1));
      assertEquals(Double.parseDouble(expectedResult[2]), tuple.get(2));
      assertEquals(Double.parseDouble(expectedResult[3]), tuple.get(3));
      assertEquals(Double.parseDouble(expectedResult[4]), tuple.get(4));
      assertEquals(Double.parseDouble(expectedResult[5]), tuple.get(5));
      count++;
    }
    assertEquals(expectedResults.size(), count);
  }
  
  public void testShouldSkipPoints() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 0, 0 0)), POINT (5 5))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, geom);\n" +
        "B = FOREACH A GENERATE geom_id, FLATTEN("+Break.class.getName()+"(geom));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Vector<String[]> expectedResults = new Vector<String[]>();
    expectedResults.add(new String[] {"1", "0", "0", "0", "0", "1"});
    expectedResults.add(new String[] {"1", "1", "0", "1", "1", "0"});
    expectedResults.add(new String[] {"1", "2", "1", "0", "0", "0"});
    Iterator<String[]> expected = expectedResults.iterator();
    int count = 0;
    while (it.hasNext() && expected.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String[] expectedResult = expected.next();
      if (tuple == null)
        break;
      assertEquals(Integer.parseInt(expectedResult[0]), tuple.get(0));
      assertEquals(Integer.parseInt(expectedResult[1]), tuple.get(1));
      assertEquals(Double.parseDouble(expectedResult[2]), tuple.get(2));
      assertEquals(Double.parseDouble(expectedResult[3]), tuple.get(3));
      assertEquals(Double.parseDouble(expectedResult[4]), tuple.get(4));
      assertEquals(Double.parseDouble(expectedResult[5]), tuple.get(5));
      count++;
    }
    assertEquals(expectedResults.size(), count);
  }
  
}
