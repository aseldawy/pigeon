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
public class TestSJPlaneSweep extends TestCase {
  
  public void testShouldWithoutDuplicateAvoidance() throws Exception {
    // Create polygons
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "1", "LINESTRING(0 0, 2 2)"});
    data.add(new String[] {"1", "2", "LINESTRING(2 1, 0 4)"});
    data.add(new String[] {"1", "3","LINESTRING(3 5, 5 3)"});
    data.add(new String[] {"2", "4", "LINESTRING(1 0, 1 5)"});
    data.add(new String[] {"2", "5", "LINESTRING(3 1, 5 1)"});
    data.add(new String[] {"2", "6", "LINESTRING(3 3, 5 5)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (gid: int, id: int, geom);\n" +
      "ldataset = FILTER A BY gid == 1;\n" +
      "rdataset = FILTER A BY gid == 2;\n" +
      "datasets = COGROUP ldataset ALL, rdataset ALL;\n"+
      "C = FOREACH datasets GENERATE FLATTEN("+SJPlaneSweep.class.getName()+"(ldataset, rdataset, null, 2, 2));\n" +
      "D = FOREACH C GENERATE $1, $4;\n";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    
    Vector<Point> correctResult = new Vector<Point>();
    correctResult.add(new Point(1, 4));
    correctResult.add(new Point(2, 4));
    correctResult.add(new Point(3, 6));

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

