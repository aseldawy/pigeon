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
public class TestExtent extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "POLYGON ((0 0, 6 0, 0 5, 0 0))"});
    data.add(new String[] {"0", "LINESTRING (2 2, 7 2, 2 6)"});
    data.add(new String[] {"0", "POINT (3 -2)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+ Extent.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the union outside Pig
    String true_mbr = "POLYGON ((0 -2, 7 -2, 7 6, 0 6, 0 -2))";
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      TestHelper.assertGeometryEqual(true_mbr, tuple.get(0));
    }
    assertEquals(1, output_size);
  }

}
