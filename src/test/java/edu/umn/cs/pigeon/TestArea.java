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
public class TestArea extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "POLYGON ((0 0, 0 3, 5 5, 10 0, 0 0))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+Area.class.getName()+"(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    ArrayList<Double> correct_result = new ArrayList<Double>();
    correct_result.add(15 + 5 + 12.5);
    Iterator<Double> areas = correct_result.iterator();
    while (it.hasNext() && areas.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      Double area = (Double) tuple.get(0);
      assertEquals(areas.next(), area);
    }
  }

}
