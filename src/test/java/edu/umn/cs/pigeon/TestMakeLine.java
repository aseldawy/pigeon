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

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;

import edu.umn.cs.pigeon.MakeLine;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestMakeLine extends TestCase {
  
  private ArrayList<OGCGeometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestMakeLine() {
    geometries = new ArrayList<OGCGeometry>();
    geometries.add(OGCGeometry.fromText("Linestring(0 0, 0 3, 4 5, 10 0)"));
    geometries.add(OGCGeometry.fromText("Linestring(5 6, 10 3, 7  13)"));
    
    data = new ArrayList<String[]>();
    for (int i_geom = 0; i_geom < geometries.size(); i_geom++) {
      OGCGeometry geom = geometries.get(i_geom);
      Polyline polyline = (Polyline) geom.getEsriGeometry();
      for (int i_point = 0; i_point < polyline.getPointCount(); i_point++) {
        Point point = polyline.getPoint(i_point);
        data.add(new String[] {Integer.toString(i_geom), Integer.toString(i_point),
            "Point ("+point.getX()+" "+point.getY()+")"});
      }
    }
  }
  
  public void testShouldWorkWithWKT() throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (geom_id, point_pos, point);\n" +
      "B = ORDER A BY point_pos;" +
      "C = GROUP B BY geom_id;" +
      "D = FOREACH C GENERATE group, "+MakeLine.class.getName()+"(B.point);";
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

}
