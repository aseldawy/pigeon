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
public class TestMakePoint extends TestCase {
  
  protected void innerTest(String schema) throws Exception {
    
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "1.0", "1.0"});
    data.add(new String[] {"1", "-1.0", "-3.55"});
    data.add(new String[] {"2", "0.0", "0.0"});

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as "+schema+";\n" +
      "B = FOREACH A GENERATE "+ MakePoint.class.getName()+"(x, y);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    ArrayList<String> points = new ArrayList<String>();
    points.add("POINT (1 1)");
    points.add("POINT (-1 -3.55)");
    points.add("POINT (0 0)");
    Iterator<String> i_point = points.iterator();
    while (it.hasNext() && i_point.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      String expected_result = i_point.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expected_result, tuple.get(0));
    }
  }

  public void testShouldMakeAPointFromDoubles() throws Exception {
    innerTest("(id:int, x:double, y:double)");
  }

  public void testShouldMakeAPointFromDataByteArray() throws Exception {
    innerTest("(id, x, y)");
  }

}
