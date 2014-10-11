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

import com.esri.core.geometry.ogc.OGCGeometry;

/**
 * @author Ahmed Eldawy
 *
 */
public class TestBuffer extends TestCase {
  
  
  
  public TestBuffer() {
    
  }
  
  public void testShouldWorkWithDoubleBuffer() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "LINESTRING (0 0, 0 3, 4 5, 10 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+ Buffer.class.getName()+"(geom, 1.0);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    ArrayList<String> geometries = new ArrayList<String>();
    geometries.add(OGCGeometry.fromText("LINESTRING (0 0, 0 3, 4 5, 10 0)").buffer(1.0).asText());
    Iterator<String> geoms = geometries.iterator();
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      String exptected_result = geoms.next();
      TestHelper.assertGeometryEqual(exptected_result, tuple.get(0));
    }
  }
  
  public void testShouldWorkWithIntegerBuffer() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "LINESTRING (0 0, 0 3, 4 5, 10 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+ Buffer.class.getName()+"(geom, 1);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    ArrayList<String> geometries = new ArrayList<String>();
    geometries.add(OGCGeometry.fromText("LINESTRING (0 0, 0 3, 4 5, 10 0)").buffer(1.0).asText());
    Iterator<String> geoms = geometries.iterator();
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      String exptected_result = geoms.next();
      TestHelper.assertGeometryEqual(exptected_result, tuple.get(0));
    }
  }

  public void testShouldWorkWithUntypedColumn() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "LINESTRING (0 0, 0 3, 4 5, 10 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+ Buffer.class.getName()+"(geom, id);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    ArrayList<String> geometries = new ArrayList<String>();
    geometries.add(OGCGeometry.fromText("LINESTRING (0 0, 0 3, 4 5, 10 0)").buffer(1.0).asText());
    Iterator<String> geoms = geometries.iterator();
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      String exptected_result = geoms.next();
      TestHelper.assertGeometryEqual(exptected_result, tuple.get(0));
    }
  }
}
