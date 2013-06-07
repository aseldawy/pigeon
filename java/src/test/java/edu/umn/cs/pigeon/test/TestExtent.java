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

import edu.umn.cs.pigeon.Extent;
import edu.umn.cs.pigeon.GeometryParser;


/**
 * @author Ahmed Eldawy
 *
 */
public class TestExtent extends TestCase {
  
  private ArrayList<OGCGeometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestExtent() {
    geometries = new ArrayList<OGCGeometry>();

    // Create polygons
    geometries.add(OGCGeometry.fromText("Polygon((0 0, 6 0, 0 5, 0 0))"));
    geometries.add(OGCGeometry.fromText("Linestring(2 2, 7 2, 2 6)"));
    geometries.add(OGCGeometry.fromText("Point(3 -2)"));

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
      "C = FOREACH B GENERATE "+Extent.class.getName()+"(A.geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    
    // Calculate the union outside Pig
    OGCGeometryCollection geometry_collection = new OGCConcreteGeometryCollection(
        geometries, geometries.get(0).getEsriSpatialReference());
    OGCGeometry true_mbr = geometry_collection.envelope();
    
    int output_size = 0;
    
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      output_size++;
      OGCGeometry calculated_mbr = new GeometryParser().parseGeom(tuple.get(0));
      assertTrue(true_mbr.equals(calculated_mbr));
    }
    assertEquals(1, output_size);
  }

}
