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
public class TestMakeSegments extends TestCase {

  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "1", "0", "POINT (0.0 0.0)"});
    data.add(new String[] {"1", "2", "1", "POINT (0.0 3.0)"});
    data.add(new String[] {"1", "3", "2", "POINT (4.0 5.0)"});
    data.add(new String[] {"1", "4", "3", "POINT (10.0 0.0)"});
    data.add(new String[] {"2", "5", "0", "POINT (5.0 6.0)"});
    data.add(new String[] {"2", "6", "1", "POINT (10.0 3.0)"});
    data.add(new String[] {"2", "7", "2", "POINT (7.0 13.0)"});
    data.add(new String[] {"3", "1", "0", "POINT (0.0 0.0)"});
    data.add(new String[] {"3", "8", "1", "POINT (10.0 10.0)"});
    data.add(new String[] {"3", "9", "2", "POINT (18.0 5.0)"});
    data.add(new String[] {"3", "1", "3", "POINT (0.0 0.0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, point_id: int, point_pos: int, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, FLATTEN("+MakeSegments.class.getName()+"(B.point_id, B.point));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    
    ArrayList<String[]> expectedResults = new ArrayList<String[]>();
    expectedResults.add(new String[] { "1", "0", "1", "0.0", "0.0", "2", "0.0", "3.0"});
    expectedResults.add(new String[] { "1", "1", "2", "0.0", "3.0", "3", "4.0", "5.0"});
    expectedResults.add(new String[] { "1", "2", "3", "4.0", "5.0", "4", "10.0", "0.0"});
    expectedResults.add(new String[] { "2", "0", "5", "5.0", "6.0", "6", "10.0", "3.0"});
    expectedResults.add(new String[] { "2", "1", "6", "10.0", "3.0", "7", "7.0", "13.0"});
    expectedResults.add(new String[] { "3", "0", "1", "0.0", "0.0", "8", "10.0", "10.0"});
    expectedResults.add(new String[] { "3", "1", "8", "10.0", "10.0", "9", "18.0", "5.0"});
    expectedResults.add(new String[] { "3", "2", "9", "18.0", "5.0", "1", "0.0", "0.0"});
    Iterator<String[]> expectedResultIter = expectedResults.iterator();
    int count = 0;
    while (it.hasNext() && expectedResultIter.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String[] expectedResult = expectedResultIter.next();
      if (tuple == null)
        break;
      assertEquals(Integer.parseInt(expectedResult[0]), tuple.get(0));
      assertEquals(Integer.parseInt(expectedResult[1]), tuple.get(1));
      assertEquals(Long.parseLong(expectedResult[2]), tuple.get(2));
      assertEquals(Double.parseDouble(expectedResult[3]), tuple.get(3));
      assertEquals(Double.parseDouble(expectedResult[4]), tuple.get(4));
      assertEquals(Long.parseLong(expectedResult[5]), tuple.get(5));
      assertEquals(Double.parseDouble(expectedResult[6]), tuple.get(6));
      assertEquals(Double.parseDouble(expectedResult[7]), tuple.get(7));
      count++;
    }
    assertEquals(expectedResults.size(), count);
  }

}
