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
public class TestMakeBox extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (dummy);\n" +
      "B = FOREACH A GENERATE "+MakeBox.class.getName()+"(1, 2, 5, 7);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Vector<String> expectedResult = new Vector<String>();
    expectedResult.add("POLYGON((1 2, 5 2, 5 7, 1 7, 1 2))");

    Iterator<String> geoms = expectedResult.iterator();
    int count = 0;
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String expected_result = geoms.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expected_result, tuple.get(0));
      count++;
    }
    assertEquals(expectedResult.size(), count);
  }

}
