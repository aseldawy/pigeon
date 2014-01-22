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
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPoint;

/**
 * @author Ahmed Eldawy
 *
 */
public class TestMakePoint extends TestCase {
  
  private ArrayList<Point> points;
  private ArrayList<String[]> data;
  
  public TestMakePoint() {
    points = new ArrayList<Point>();
    points.add(new Point(1, 1));
    points.add(new Point(-1, -3.55));
    points.add(new Point(0, 0));

    data = new ArrayList<String[]>();
    for (int i = 0; i < points.size(); i++) {
      Point point = points.get(i);
      data.add(new String[] { Integer.toString(i),
          Double.toString(point.getX()), Double.toString(point.getY()) });
    }
  }
  
  protected void innerTest(String schema) throws Exception {
    String datafile = TestHelper.createTempFile(data, "\t");
    datafile = datafile.replace("\\", "\\\\");
    PigServer pig = new PigServer(LOCAL);
    String query = "A = LOAD 'file:" + datafile + "' as "+schema+";\n" +
      "B = FOREACH A GENERATE "+MakePoint.class.getName()+"(x, y);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("B");
    Iterator<Point> i_point = points.iterator();
    while (it.hasNext() && i_point.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      Point point = i_point.next();
      if (tuple == null)
        break;
      DataByteArray created_point = (DataByteArray) tuple.get(0);
      assertTrue(Arrays.equals(created_point.get(),
          new OGCPoint(point, SpatialReference.create(4326)).asBinary().array()));
    }
  }

  public void testShouldMakeAPointFromDoubles() throws Exception {
    innerTest("(id:int, x:double, y:double)");
  }

  public void testShouldMakeAPointFromDataByteArray() throws Exception {
    innerTest("(id, x, y)");
  }

}
