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
public class TestConvexHull extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "POLYGON ((0 0, 6 0, 0 5, 0 0))"});
    data.add(new String[] {"1", "POLYGON ((2 2, 7 2, 2 6, 2 2))"});
    data.add(new String[] {"2", "POLYGON ((3 -2, 8 -1, 8 4, 3 -2))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+ ConvexHull.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the convex hull outside Pig
    String true_convex_hull = "POLYGON ((0 0, 3 -2, 8 -1, 8 4, 2 6, 0 5, 0 0))";
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      TestHelper.assertGeometryEqual(true_convex_hull, tuple.get(0));
    }
    assertEquals(1, output_size);
  }

  public void testShouldWorkWithOneObject() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "POLYGON ((0 0, 0 2, 1 1, 2 2, 2 0, 0 0))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+ConvexHull.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the convex hull outside Pig
    String true_convex_hull = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))";
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      TestHelper.assertGeometryEqual(true_convex_hull, tuple.get(0));
    }
    assertEquals(1, output_size);
  }

  public void testShouldWorkAsUnaryOperation() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "POLYGON ((0 0, 6 0, 0 5, 0 0))"});
    data.add(new String[] {"1", "POLYGON ((0 0, 0 2, 1 1, 2 2, 2 0, 0 0))"});
    data.add(new String[] {"2", "POLYGON ((3 -2, 8 -1, 8 4, 3 -2))"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);

    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+ConvexHull.class.getName()+"(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    int output_size = 0;

    ArrayList<String> expected_results = new ArrayList<String>();
    expected_results.add("POLYGON ((0 0, 6 0, 0 5, 0 0))");
    expected_results.add("POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))");
    expected_results.add("POLYGON ((3 -2, 8 -1, 8 4, 3 -2))");
    Iterator<String> geoms = expected_results.iterator();
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      TestHelper.assertGeometryEqual(geoms.next(), tuple.get(0));
    }
    assertEquals(3, output_size);
  }

}
