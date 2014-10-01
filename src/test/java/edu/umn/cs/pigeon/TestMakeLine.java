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
 * @author Ahmed Eldawy
 *
 */
public class TestMakeLine extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "0", "POINT (0.0 0.0)"});
    data.add(new String[] {"0", "1", "POINT (0.0 3.0)"});
    data.add(new String[] {"0", "2", "POINT (4.0 5.0)"});
    data.add(new String[] {"0", "3", "POINT (10.0 0.0)"});
    data.add(new String[] {"1", "0", "POINT (5.0 6.0)"});
    data.add(new String[] {"1", "1", "POINT (10.0 3.0)"});
    data.add(new String[] {"1", "2", "POINT (7.0 13.0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id, point_pos, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, "+MakeLine.class.getName()+"(B.point);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    Vector<String> expectedResult = new Vector<String>();
    expectedResult.add("LINESTRING(0 0, 0 3, 4 5, 10 0)");
    expectedResult.add("LINESTRING(5 6, 10 3, 7  13)");

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

}
