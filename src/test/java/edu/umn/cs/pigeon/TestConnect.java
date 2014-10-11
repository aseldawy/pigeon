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

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestConnect extends TestCase {
  
  public void testShouldWorkWithDirectPolygon() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "2", "LINESTRING(0 0, 2 0, 3 1)"});
    data.add(new String[] {"2", "3", "LINESTRING(3 1, 2 2)"});
    data.add(new String[] {"3", "1", "LINESTRING(2 2, 1 2, 0 0)"});

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' AS (first_point: long, last_point: long, linestring: chararray);\n" +
      "B = GROUP A ALL;" +
      "C = FOREACH B GENERATE "+Connect.class.getName()+"(A.first_point, A.last_point, A.linestring);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    String expectedResult = "Polygon((0 0, 2 0, 3 1, 2 2, 1 2, 0 0))";
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expectedResult, tuple.get(0));
      count++;
    }
    assertEquals(1, count);
  }
  
  
  public void testShouldWorkWithSomeReversedSegments() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "2", "LINESTRING(0 0, 2 0, 3 1)"});
    data.add(new String[] {"3", "2", "LINESTRING(2 2, 3 1)"});
    data.add(new String[] {"3", "1", "LINESTRING(2 2, 1 2, 0 0)"});

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' AS (first_point: long, last_point: long, linestring: chararray);\n" +
      "B = GROUP A ALL;" +
      "C = FOREACH B GENERATE "+Connect.class.getName()+"(A.first_point, A.last_point, A.linestring);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    String expectedResult = "Polygon((0 0, 2 0, 3 1, 2 2, 1 2, 0 0))";
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expectedResult, tuple.get(0));
      count++;
    }
    assertEquals(1, count);
  }

  public void testShouldMakeGeometryCollectionForDisconnectedShapes() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "2", "LINESTRING(0 0, 2 0, 3 1)"});
    data.add(new String[] {"2", "3", "LINESTRING(3 1, 2 2)"});
    data.add(new String[] {"3", "1", "LINESTRING(2 2, 1 2, 0 0)"});
    data.add(new String[] {"7", "8", "LINESTRING(10 8, 8 5)"});

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' AS (first_point: long, last_point: long, linestring: chararray);\n" +
        "B = GROUP A ALL;" +
        "C = FOREACH B GENERATE "+Connect.class.getName()+"(A.first_point, A.last_point, A.linestring);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    int count = 0;
    String expectedResult = "GEOMETRYCOLLECTION(LINESTRING(10 8, 8 5), "
        + "POLYGON((0 0, 2 0, 3 1, 2 2, 1 2, 0 0)))";
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expectedResult, tuple.get(0));
      count++;
    }
    assertEquals(1, count);
  }
  
  public void testShouldWorkWithEntriesOfTypePolygon() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "1", "POLYGON((0 0, 2 0, 3 1, 0 0))"});
    data.add(new String[] {"2", "2", "POLYGON((5 5, 5 6, 6 5, 5 5))"});
    data.add(new String[] {"7", "8", "LINESTRING(10 8, 8 5)"});
    String expectedResult = "GeometryCollection(LineString(10 8, 8 5), "
        + "Polygon((0 0, 2 0, 3 1, 0 0)), Polygon((5 5, 5 6, 6 5, 5 5)))";

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' AS (first_point: long, last_point: long, linestring: chararray);\n" +
        "B = GROUP A ALL;" +
        "C = FOREACH B GENERATE "+Connect.class.getName()+"(A.first_point, A.last_point, A.linestring);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expectedResult, tuple.get(0));
      count++;
    }
    assertEquals(1, count);
  }
}
