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
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

import edu.umn.cs.pigeon.ConvexHull;
import edu.umn.cs.pigeon.GeometryParser;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestConvexHull extends TestCase {
  
  private ArrayList<OGCGeometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestConvexHull() {
    geometries = new ArrayList<OGCGeometry>();

    // Create polygons
    geometries.add(OGCGeometry.fromText("Polygon((0 0, 6 0, 0 5, 0 0))"));
    geometries.add(OGCGeometry.fromText("Polygon((2 2, 7 2, 2 6, 2 2))"));
    geometries.add(OGCGeometry.fromText("Polygon((3 -2, 8 -1, 8 4, 3 -2))"));

    data = new ArrayList<String[]>();
    for (int i = 0; i < geometries.size(); i++) {
      data.add(new String[] {Integer.toString(i), geometries.get(i).asText()});
    }
  }
  
  public void testShouldWorkWithWKT() throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+ConvexHull.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the convex hull outside Pig
    OGCGeometryCollection geometry_collection = new OGCConcreteGeometryCollection(
        geometries, geometries.get(0).getEsriSpatialReference());
    OGCGeometry true_convex_hull = geometry_collection.convexHull();
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      OGCGeometry calculated_convex_hull = new GeometryParser().parseGeom(tuple.get(0));
      assertTrue(true_convex_hull.equals(calculated_convex_hull));
    }
    assertEquals(1, output_size);
  }

  public void testShouldWorkWithOneObject() throws Exception {
    OGCGeometry geom =
        OGCGeometry.fromText("Polygon ((0 0, 0 2, 1 1, 2 2, 2 0, 0 0))");
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", geom.asText()});
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom);\n" +
      "B = GROUP A ALL;\n" +
      "C = FOREACH B GENERATE "+ConvexHull.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the convex hull outside Pig
    OGCGeometry true_convex_hull = geom.convexHull();
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      OGCGeometry calculated_convex_hull = new GeometryParser().parseGeom(tuple.get(0));
      assertTrue(true_convex_hull.equals(calculated_convex_hull));
    }
    assertEquals(1, output_size);
  }

}
