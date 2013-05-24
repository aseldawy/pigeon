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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * Generates a geometry of type LineString out of a bag of points.
 * @author Ahmed Eldawy
 */
public class ST_MakeLine extends EvalFunc<DataByteArray>{
  
  private GeometryFactory geometryFactory = new GeometryFactory();
  private GeometryParser geometryParser = new GeometryParser();
  private WKBWriter wkbWriter = new WKBWriter();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    DataBag points = (DataBag) b.get(0);
    Coordinate[] coordinates = new Coordinate[(int) points.size()];
    int i = 0;
    for (Tuple t : points) {
      try {
        Geometry point = geometryParser.parseGeom(t.get(0));
        coordinates[i++] = point.getCoordinate();
      } catch (ParseException e) {
        throw new IOException("Error parsing "+t.get(0), e);
      }
    }
    Geometry line = geometryFactory.createLineString(coordinates);
    return new DataByteArray(wkbWriter.write(line));
  }

}
