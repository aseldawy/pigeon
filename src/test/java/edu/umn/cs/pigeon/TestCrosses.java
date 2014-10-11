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
public class TestCrosses extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "LINESTRING (0 0, 3 3)", "LINESTRING (0 3, 3 0)"});
    data.add(new String[] {"1", "LINESTRING (0 0, 0 3)", "LINESTRING (0 3, 3 0)"});
    data.add(new String[] {"2", "LINESTRING (0 0, 3 0)", "LINESTRING (0 3, 3 3)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id: int, geom1, geom2);\n" +
      "B = FILTER A BY "+Crosses.class.getName()+"(geom1, geom2);";
    pig.registerQuery(query);

    Iterator<?> it = pig.openIterator("B");
    
    ArrayList<Integer> expected_results = new ArrayList<Integer>();
    expected_results.add(0);
    Iterator<Integer> result_ids = expected_results.iterator();
    int count = 0;
    while (it.hasNext() && result_ids.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      count++;
      assertEquals(result_ids.next(), (Integer)tuple.get(0));
    }
    assertFalse(it.hasNext());
    assertEquals(1, count);
  }
}
