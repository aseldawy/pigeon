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
public class TestDifference extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))", "POLYGON ((4 2, 4 4, 2 4, 4 2))"});
    data.add(new String[] {"1", "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))", "POLYGON ((4 0, 4 2, 2 2, 4 0))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom1, geom2);\n" +
      "B = FOREACH A GENERATE "+ Difference.class.getName()+"(geom1, geom2);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    ArrayList<String> expected_results = new ArrayList<String>();
    expected_results.add("POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))");
    expected_results.add("POLYGON ((0 0, 3 0, 3 1, 2 2, 3 2, 3 3, 0 3, 0 0))");
    Iterator<String> geom = expected_results.iterator();
    
    while (it.hasNext() && geom.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(geom.next(), tuple.get(0));
    }
  }
}
