/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.pigeon;

import junit.framework.TestCase;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import java.util.ArrayList;
import java.util.Iterator;

import static org.apache.pig.ExecType.LOCAL;

/**
 * @author Ahmed Eldawy
 *
 */
public class TestXMin extends TestCase {
  
  public void testShouldWorkWithPoints() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "POINT (0 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+XMin.class.getName()+"(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    ArrayList<Double> correct_result = new ArrayList<Double>();
    correct_result.add(0.0);
    Iterator<Double> xmins = correct_result.iterator();
    while (it.hasNext() && xmins.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      Double xmin = (Double) tuple.get(0);
      assertEquals(xmins.next(), xmin);
    }
  }

}
