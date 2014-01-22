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

import com.esri.core.geometry.ogc.OGCGeometry;

/**
 * @author Ahmed Eldawy
 *
 */
public class TestCrosses extends TestCase {
  
  private ArrayList<OGCGeometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestCrosses() {
    geometries = new ArrayList<OGCGeometry>();
    geometries.add(OGCGeometry.fromText("Linestring (0 0, 3 3)"));
    geometries.add(OGCGeometry.fromText("Linestring (0 3, 3 0)"));
    geometries.add(OGCGeometry.fromText("Linestring (0 0, 0 3)"));
    geometries.add(OGCGeometry.fromText("Linestring (0 3, 3 0)"));
    geometries.add(OGCGeometry.fromText("Linestring (0 0, 3 0)"));
    geometries.add(OGCGeometry.fromText("Linestring (0 3, 3 3)"));

    data = new ArrayList<String[]>();
    for (int i = 0; i < geometries.size(); i += 2) {
      data.add(new String[] {Integer.toString(i), geometries.get(i).asText(),
          geometries.get(i+1).asText()});
    }
  }
  
  public void testShouldWorkWithWKT() throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as (id, geom1, geom2);\n" +
      "B = FILTER A BY "+Crosses.class.getName()+"(geom1, geom2);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Iterator<OGCGeometry> geoms = geometries.iterator();
    int count = 0;
    while (it.hasNext() && geoms.hasNext()) {
      it.next();
      count++;
    }
    assertEquals(1, count);
  }
}
