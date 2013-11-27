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

import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCGeometry;

import edu.umn.cs.pigeon.Area;

/**
 * @author Ahmed Eldawy
 *
 */
public class TestArea extends TestCase {
  
  private ArrayList<OGCGeometry> geometries;
  private ArrayList<String[]> data;
  
  
  public TestArea() {
    geometries = new ArrayList<OGCGeometry>();
    geometries.add(OGCGeometry.fromText("Polygon ((0 0, 0 3, 4 5, 10 0, 0 0))"))
    ;
    
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
      "B = FOREACH A GENERATE "+Area.class.getName()+"(geom);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Iterator<OGCGeometry> geoms = geometries.iterator();
    while (it.hasNext() && geoms.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      OGCGeometry geom = geoms.next();
      if (tuple == null)
        break;
      Double area = (Double) tuple.get(0);
      assertEquals(((Polygon)geom.getEsriGeometry()).calculateArea2D(), area);
    }
  }

}
