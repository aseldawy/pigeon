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
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import edu.umn.cs.spig.GeometryParser;
import edu.umn.cs.spig.ST_Union;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestST_Union extends TestCase {
  
  private ArrayList<Geometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestST_Union() {
    geometries = new ArrayList<Geometry>();
    GeometryFactory geometryFactory = new GeometryFactory();

    // Create polygons
    geometries.add(geometryFactory.createPolygon(
        geometryFactory.createLinearRing(new Coordinate[] {
            new Coordinate(0, 0), new Coordinate(6, 0), new Coordinate(0, 5),
            new Coordinate(0, 0) }), null));
    geometries.add(geometryFactory.createPolygon(
        geometryFactory.createLinearRing(new Coordinate[] {
            new Coordinate(2, 2), new Coordinate(7, 2), new Coordinate(2, 6),
            new Coordinate(2, 2) }), null));
    geometries.add(geometryFactory.createPolygon(
        geometryFactory.createLinearRing(new Coordinate[] {
            new Coordinate(3, -2), new Coordinate(8, -1), new Coordinate(8, 4),
            new Coordinate(3, -2) }), null));

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
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+ST_Union.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the union outside Pig
    GeometryFactory geometryFactory = new GeometryFactory();
    GeometryCollection all_geoms = geometryFactory.createGeometryCollection(
        geometries.toArray(new Geometry[geometries.size()]));
    Geometry true_union = all_geoms.buffer(0);
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      Geometry calculated_union = new GeometryParser().parse(tuple.get(0));
      assertTrue(true_union.equals(calculated_union));
    }
    assertEquals(1, output_size);
  }

}
