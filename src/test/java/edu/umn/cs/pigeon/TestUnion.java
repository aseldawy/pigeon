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
public class TestUnion extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    // Create polygons
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "POLYGON((0 0, 6 0, 0 6, 0 0))"});
    data.add(new String[] {"1", "POLYGON((3 2, 8 2, 3 7, 3 2))"});
    data.add(new String[] {"2", "POLYGON((2 -2, 9 -2, 9 5, 2 -2))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+Union.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    String true_union = "POLYGON((4 0, 2 -2, 9 -2, 9 5, 7 3, 3 7, 3 3, 0 6, 0 0, 4 0),"
        + " (5 1, 4 2, 6 2, 5 1))";
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      TestHelper.assertGeometryEqual(true_union, tuple.get(0));
    }
    assertEquals(1, output_size);
  }

}
