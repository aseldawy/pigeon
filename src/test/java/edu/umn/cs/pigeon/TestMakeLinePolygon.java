/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package edu.umn.cs.pigeon;

import static org.apache.pig.ExecType.LOCAL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestMakeLinePolygon extends TestCase {
  
  private ArrayList<OGCGeometry> geometries;
  private ArrayList<String[]> data;
  
  public TestMakeLinePolygon() {
    geometries = new ArrayList<OGCGeometry>();
    geometries.add(OGCGeometry.fromText("Linestring(0 0, 0 3, 4 5, 10 0)"));
    geometries.add(OGCGeometry.fromText("Linestring(5 6, 10 3, 7  13)"));
    geometries.add(OGCGeometry.fromText("Polygon((0 0, 10 10, 18 5, 0 0))"));
    
    data = new ArrayList<String[]>();
    for (int i_geom = 0; i_geom < geometries.size(); i_geom++) {
      OGCGeometry geom = geometries.get(i_geom);
      boolean is_polygon = geom.getEsriGeometry() instanceof Polygon;
      MultiPath multipath = (MultiPath) geom.getEsriGeometry();
      for (int i_point = 0; i_point < multipath.getPointCount(); i_point++) {
        Point point = multipath.getPoint(i_point);
        data.add(new String[] {
            Integer.toString(i_geom+1), // Geometry ID
            Integer.toString(i_point+1), // Point ID
            Integer.toString(i_point), // point position
            "Point ("+point.getX()+" "+point.getY()+")"}); // Point location
      }
      if (is_polygon) {
        // Replicate first point with same information to close the polygon
        Point first_point = multipath.getPoint(0);
        data.add(new String[] {
            Integer.toString(i_geom+1), // Geometry ID
            Integer.toString(1), // Point ID
            Integer.toString(multipath.getPointCount()), // point position
            "Point ("+first_point.getX()+" "+first_point.getY()+")"}); // Point location
      }
    }
  }
  
  public void testShouldWorkWithWKT() throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, point_id: int, point_pos: int, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, "+MakeLinePolygon.class.getName()+"(B.point_id, B.point);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    Iterator<OGCGeometry> geoms = geometries.iterator();
    int count = 0;
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      OGCGeometry geom = geoms.next();
      if (tuple == null)
        break;
      assertTrue(Arrays.equals(geom.asBinary().array(), ((DataByteArray)tuple.get(1)).get()));
      count++;
    }
    assertEquals(geometries.size(), count);
  }

  public void testShouldFallBackToLinestringForShortLists() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "1", "0", "Point (0 0)"});
    data.add(new String[] {"1", "2", "1", "Point (4 5)"});
    data.add(new String[] {"1", "1", "2", "Point (0 0)"});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id: int, point_id: int, point_pos: int, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, "+MakeLinePolygon.class.getName()+"(B.point_id, B.point);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      OGCGeometry geom = OGCGeometry.fromText("Linestring(0 0, 4 5)");
      assertTrue(Arrays.equals(geom.asBinary().array(), ((DataByteArray)tuple.get(1)).get()));
      count++;
    }
    assertEquals(1, count);
  }

}
