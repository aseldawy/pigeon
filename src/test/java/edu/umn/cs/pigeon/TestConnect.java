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


/**
 * @author Ahmed Eldawy
 *
 */
public class TestConnect extends TestCase {
  
  public void testShouldWorkWithDirectPolygon() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "2", "Linestring(0 0, 2 0, 3 1)"});
    data.add(new String[] {"2", "3", "Linestring(3 1, 2 2)"});
    data.add(new String[] {"3", "1", "Linestring(2 2, 1 2, 0 0)"});

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' AS (first_point: long, last_point: long, linestring: chararray);\n" +
      "B = GROUP A ALL;" +
      "C = FOREACH B GENERATE "+Connect.class.getName()+"(A.first_point, A.last_point, A.linestring);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    OGCGeometry expectedResult = OGCGeometry.fromText("Polygon((0 0, 2 0, 3 1, 2 2, 1 2, 0 0))");
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      assertTrue(expectedResult.equals(new GeometryParser().parseGeom(tuple.get(0))));
      count++;
    }
    assertEquals(1, count);
  }
  
  
  public void testShouldWorkWithSomeReversedSegments() throws Exception {
    ArrayList<String[]> data = new ArrayList<String[]>();
    data.add(new String[] {"1", "2", "Linestring(0 0, 2 0, 3 1)"});
    data.add(new String[] {"3", "2", "Linestring(2 2, 3 1)"});
    data.add(new String[] {"3", "1", "Linestring(2 2, 1 2, 0 0)"});

    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' AS (first_point: long, last_point: long, linestring: chararray);\n" +
      "B = GROUP A ALL;" +
      "C = FOREACH B GENERATE "+Connect.class.getName()+"(A.first_point, A.last_point, A.linestring);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("C");
    OGCGeometry expectedResult = OGCGeometry.fromText("Polygon((0 0, 2 0, 3 1, 2 2, 1 2, 0 0))");
    int count = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      assertTrue(expectedResult.equals(new GeometryParser().parseGeom(tuple.get(0))));
      count++;
    }
    assertEquals(1, count);
  }

}
