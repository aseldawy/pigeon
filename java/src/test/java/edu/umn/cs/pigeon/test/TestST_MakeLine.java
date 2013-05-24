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

package edu.umn.cs.pigeon.test;

import static org.apache.pig.ExecType.LOCAL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKBWriter;

import edu.umn.cs.pigeon.ST_MakeLine;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestST_MakeLine extends TestCase {
  
  private ArrayList<Geometry> geometries;
  private ArrayList<String[]> data;
  private WKBWriter wkbWriter = new WKBWriter();
  
  
  public TestST_MakeLine() {
    GeometryFactory geometryFactory = new GeometryFactory();
    geometries = new ArrayList<Geometry>();
    geometries.add(geometryFactory.createLineString(new Coordinate[] {
        new Coordinate(0, 0), new Coordinate(0, 3), new Coordinate(4, 5),
        new Coordinate(10, 0) }));
    geometries.add(geometryFactory.createLineString(new Coordinate[] {
        new Coordinate(5, 6), new Coordinate(10, 3), new Coordinate(7, 13)}));
    
    data = new ArrayList<String[]>();
    for (int i_geom = 0; i_geom < geometries.size(); i_geom++) {
      Coordinate[] coordinates = geometries.get(i_geom).getCoordinates();
      for (int i_point = 0; i_point < coordinates.length; i_point++) {
        Point point = geometryFactory.createPoint(coordinates[i_point]);
        data.add(new String[] {Integer.toString(i_geom), Integer.toString(i_point), point.toText()});
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
      "D = FOREACH C GENERATE group, "+ST_MakeLine.class.getName()+"(B.point);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("D");
    Iterator<Geometry> geoms = geometries.iterator();
    int count = 0;
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      Geometry geom = geoms.next();
      if (tuple == null)
        break;
      assertTrue(Arrays.equals(wkbWriter.write(geom), ((DataByteArray)tuple.get(1)).get()));
      count++;
    }
    assertEquals(geometries.size(), count);
  }

}
