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
public class TestNumPoints extends TestCase {
  
  public void testShouldWorkWithGeometries() throws Exception {
    // Create polygons
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "LINESTRING(0 0, 6 0, 0 6, 0 0)"});
    data.add(new String[] {"1", "POLYGON((3 2, 8 2, 3 7, 3 2))"});
    data.add(new String[] {"2", "POINT(3 2)"});
    data.add(new String[] {"3", "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(2 -2, 9 -2, 9 5, 2 10))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+NumPoints.class.getName()+"(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    int output_size = 0;
    int[] correct_sizes = {4, 3, 1, 5};
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      assertEquals(correct_sizes[output_size], (int)(Integer)tuple.get(0));
      output_size++;
    }
    assertEquals(correct_sizes.length, output_size);
  }

}
