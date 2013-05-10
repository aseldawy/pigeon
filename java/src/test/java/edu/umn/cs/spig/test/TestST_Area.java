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

package edu.umn.cs.spig.test;

import static org.apache.pig.ExecType.LOCAL;

import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

import edu.umn.cs.spig.ST_Area;


/**
 * @author eldawy
 *
 */
public class TestST_Area extends TestCase {
  
  private ArrayList<Geometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestST_Area() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(0, 0);
    coordinates[1] = new Coordinate(0, 3);
    coordinates[2] = new Coordinate(4, 5);
    coordinates[3] = new Coordinate(10, 0);
    coordinates[4] = new Coordinate(0, 0);
    LinearRing line = geometryFactory.createLinearRing(coordinates);
    geometries = new ArrayList<Geometry>();
    geometries.add(geometryFactory.createPolygon(line, null));
    
    data = new ArrayList<String[]>();
    for (int i = 0; i < geometries.size(); i++) {
      data.add(new String[] {Integer.toString(i), geometries.get(i).toText()});
    }
  }
  
  public void testShouldWorkWithWKT() throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = FOREACH A GENERATE "+ST_Area.class.getName()+"(geom);";
    System.out.println(query);
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Iterator<Geometry> geoms = geometries.iterator();
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      Geometry geom = geoms.next();
      if (tuple == null)
        break;
      Double area = (Double) tuple.get(0);
      assertEquals(geom.getArea(), area);
    }
  }

}
