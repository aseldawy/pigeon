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
public class TestBounds extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    // Create polygons
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "POLYGON((3 2, 8 2, 3 7, 3 2))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n"
        + "B = FOREACH A GENERATE " + XMin.class.getName() + "(geom), "
        + YMin.class.getName() + "(geom), " + XMax.class.getName() + "(geom), "
        + YMax.class.getName() + "(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    double[] correct_bounds = {3.0, 2.0, 8.0, 7.0};
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      for (int i = 0; i < correct_bounds.length; i++)
        assertEquals(correct_bounds[i], (double)(Double)tuple.get(i));
    }
  }

}
