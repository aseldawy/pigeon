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
public class TestMakeLinePolygon extends TestCase {

  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "1", "0", "POINT (0.0 0.0)"});
    data.add(new String[] {"1", "2", "1", "POINT (0.0 3.0)"});
    data.add(new String[] {"1", "3", "2", "POINT (4.0 5.0)"});
    data.add(new String[] {"1", "4", "3", "POINT (10.0 0.0)"});
    data.add(new String[] {"2", "1", "0", "POINT (5.0 6.0)"});
    data.add(new String[] {"2", "2", "1", "POINT (10.0 3.0)"});
    data.add(new String[] {"2", "3", "2", "POINT (7.0 13.0)"});
    data.add(new String[] {"3", "1", "0", "POINT (0.0 0.0)"});
    data.add(new String[] {"3", "2", "1", "POINT (10.0 10.0)"});
    data.add(new String[] {"3", "3", "2", "POINT (18.0 5.0)"});
    data.add(new String[] {"3", "1", "3", "POINT (0.0 0.0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, point_id: int, point_pos: int, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, "+MakeLinePolygon.class.getName()+"(B.point_id, B.point);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    
    ArrayList<String> expectedResult = new ArrayList<String>();
    expectedResult.add("LINESTRING(0 0, 0 3, 4 5, 10 0)");
    expectedResult.add("LINESTRING(5 6, 10 3, 7  13)");
    expectedResult.add("POLYGON((0 0, 10 10, 18 5, 0 0))");
    Iterator<String> geoms = expectedResult.iterator();
    int count = 0;
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String expected_result = geoms.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expected_result, tuple.get(1));
      count++;
    }
    assertEquals(expectedResult.size(), count);
  }

  public void testShouldFallBackToLinestringForShortLists() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "1", "0", "POINT (0 0)"});
    data.add(new String[] {"1", "2", "1", "POINT (4 5)"});
    data.add(new String[] {"1", "1", "2", "POINT (0 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, point_id: int, point_pos: int, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, "+AsText.class.getName()+"("+
        MakeLinePolygon.class.getName()+"(B.point_id, B.point));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      String expected_result = "LINESTRING(0 0, 4 5)";
      TestHelper.assertGeometryEqual(expected_result, tuple.get(1));
      count++;
    }
    assertEquals(1, count);
  }

}
