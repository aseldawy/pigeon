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
public class TestEnvelope extends TestCase {
  
  public void testShouldWorkWithWKT() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"0", "LINESTRING (0 0, 0 3, 4 5, 10 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+ Envelope.class.getName()+"(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    
    ArrayList<String> geometries = new ArrayList<String>();
    geometries.add("POLYGON ((0 0, 10 0, 10 5, 0 5, 0 0))");
    Iterator<String> geoms = geometries.iterator();
    while (it.hasNext() && geoms.hasNext()) {
      String expected_result = geoms.next();
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      TestHelper.assertGeometryEqual(expected_result, tuple.get(0));
    }
  }

}
