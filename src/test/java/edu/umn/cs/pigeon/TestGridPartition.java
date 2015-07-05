/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.pigeon;

import static org.apache.pig.ExecType.LOCAL;

import java.awt.Point;
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
public class TestGridPartition extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    // Create polygons
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "LINESTRING(0.5 0.5, 1.5 1.5)"});
    data.add(new String[] {"2", "POINT(0.5 1.5)"});
    data.add(new String[] {"3", "LINESTRING(0.5 0.5, 0.5 1.5)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id: int, geom);\n" +
      "B = FOREACH A GENERATE id, FLATTEN("+
        GridPartition.class.getName()+"(geom, 'MULTIPOINT(0 0, 2 2)', 2));";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    Vector<Point> correctResult = new Vector<Point>();
    correctResult.add(new Point(1, 0));
    correctResult.add(new Point(1, 1));
    correctResult.add(new Point(1, 2));
    correctResult.add(new Point(1, 3));
    correctResult.add(new Point(2, 2));
    correctResult.add(new Point(3, 0));
    correctResult.add(new Point(3, 2));

    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      Point resultPair = new Point((Integer)tuple.get(0), (Integer)tuple.get(1));
      assertTrue("Could not find the pair "+resultPair,
          correctResult.remove(resultPair));
    }
    assertTrue(correctResult.isEmpty());
  }

}

