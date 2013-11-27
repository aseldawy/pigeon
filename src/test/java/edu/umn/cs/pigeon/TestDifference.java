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
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;

import edu.umn.cs.pigeon.Difference;
import edu.umn.cs.pigeon.GeometryParser;

/**
 * @author Ahmed Eldawy
 *
 */
public class TestDifference extends TestCase {
  
  private ArrayList<OGCGeometry> geometries1;
  private ArrayList<OGCGeometry> geometries2;
  private ArrayList<String[]> data;
  
  
  public TestDifference() {
    geometries1 = new ArrayList<OGCGeometry>();
    geometries2 = new ArrayList<OGCGeometry>();
    geometries1.add(OGCGeometry.fromText("Polygon ((0 0, 0 3, 3 3, 3 0, 0 0))"));
    geometries2.add(OGCGeometry.fromText("Polygon ((4 2, 4 4, 2 4, 4 2))"));
    geometries1.add(OGCGeometry.fromText("Polygon ((0 0, 0 3, 3 3, 3 0, 0 0))"));
    geometries2.add(OGCGeometry.fromText("Polygon ((7 2, 7 4, 5 4, 7 2))"));

    data = new ArrayList<String[]>();
    for (int i = 0; i < geometries1.size(); i++) {
      data.add(new String[] {Integer.toString(i), geometries1.get(i).asText(),
          geometries2.get(i).asText()});
    }
  }
  
  public void testShouldWorkWithWKT() throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom1, geom2);\n" +
      "B = FOREACH A GENERATE "+Difference.class.getName()+"(geom1, geom2);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Iterator<OGCGeometry> geoms1 = geometries1.iterator();
    Iterator<OGCGeometry> geoms2 = geometries2.iterator();
    while (it.hasNext() && geoms1.hasNext() && geoms2.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      OGCGeometry pig_result = new GeometryParser().parseGeom(tuple.get(0));
      OGCGeometry true_result = geoms1.next().difference(geoms2.next());
      assertTrue(true_result.equals(pig_result));
    }
  }
}
