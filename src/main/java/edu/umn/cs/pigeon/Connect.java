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
import java.util.Vector;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.Line;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Segment;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPolygon;

/**
 * Connects together a set of Linestrings to form a longer Linestring or
 * a polygon.
 * @author Ahmed Eldawy
 */
public class Connect extends EvalFunc<DataByteArray>{
  
  private GeometryParser geometryParser = new GeometryParser();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    // Read information from input
    Vector<Long> firstPointId = new Vector<Long>();
    for (Tuple t : (DataBag) b.get(0)) {
      firstPointId.add((long) t.get(0));
    }
    
    Vector<Long> lastPointId = new Vector<Long>();
    for (Tuple t : (DataBag) b.get(1)) {
      lastPointId.add((long) t.get(0));
    }

    int sumPoints = 0;
    Vector<OGCLineString> linestrings = new Vector<OGCLineString>();
    for (Tuple t : (DataBag) b.get(2)) {
      linestrings.add((OGCLineString) geometryParser.parseGeom(t.get(0)));
      sumPoints += linestrings.lastElement().numPoints();
    }
    
    if (firstPointId.size() != lastPointId.size() ||
        firstPointId.size() != linestrings.size()) {
      throw new ExecException("All parameters should be of the same size ("
          + firstPointId.size() + "," + lastPointId.size() + ","
          + linestrings.size() + ")");
    }

    // Keep track of the first and last point IDs for the connected list
    Vector<OGCLineString> connected_lines = new Vector<OGCLineString>();
    connected_lines.add(linestrings.remove(0));
    // Which linestrings to reverse upon connection
    Vector<Boolean> reverse = new Vector<Boolean>();
    reverse.add(false);
    long first_point_id = firstPointId.remove(0);
    long last_point_id = lastPointId.remove(0);
    
    // Reorder linestrings to form a contiguous list of connected linestrings
    while (!linestrings.isEmpty()) {
      // Loop invariant:
      // At the beginning of each iteration, the lines in connected_lines are connected.
      // In each iteration, we move one linestring from linestrings to connected_lines
      // while keeping them connected
      int size_before = connected_lines.size();
      for (int i = 0; i < linestrings.size();) {
        if (lastPointId.get(i) == first_point_id) {
          // This linestring goes to the beginning of the list as-is
          lastPointId.remove(i);
          first_point_id = firstPointId.remove(i);
          connected_lines.add(0, linestrings.remove(i));
          reverse.add(0, false);
        } else if (firstPointId.get(i) == first_point_id) {
          // Should go to the beginning after being reversed
          firstPointId.remove(i);
          first_point_id = lastPointId.remove(i);
          connected_lines.add(0, linestrings.remove(i));
          reverse.add(0, true);
        } else if (firstPointId.get(i) == last_point_id) {
          // This linestring goes to the end of the list as-is
          firstPointId.remove(i);
          last_point_id = lastPointId.remove(i);
          connected_lines.add(linestrings.remove(i));
          reverse.add(false);
        } else if (lastPointId.get(i) == last_point_id) {
          // Should go to the end after being reversed
          lastPointId.remove(i);
          last_point_id = firstPointId.remove(i);
          connected_lines.add(linestrings.remove(i));
          reverse.add(true);
        } else {
          i++;
        }
      }
      if (connected_lines.size() == size_before) {
        throw new ExecException("Cannot connect any more lines to the block: "+first_point_id+","+last_point_id);
      }
    }
    
    if (first_point_id == last_point_id) {
      // A polygon
      Point[] points = new Point[sumPoints - connected_lines.size()];
      int n = 0;
      for (int i = 0; i < connected_lines.size(); i++) {
        OGCLineString linestring = connected_lines.get(i);
        boolean isReverse = reverse.get(i);
        for (int i_point = 0; i_point < linestring.numPoints() - 1; i_point++) {
          points[n++] = (Point) linestring.pointN(
              isReverse? linestring.numPoints() - 1 - i_point : i_point
              ).getEsriGeometry();
        }
      }
      Polygon multi_path = new Polygon();
      for (int i = 1; i <points.length; i++) {
        Segment segment = new Line();
        segment.setStart(points[i-1]);
        segment.setEnd(points[i]);
        multi_path.addSegment(segment, false);
      }
      OGCPolygon polygon = new OGCPolygon(multi_path, 0,
          SpatialReference.create(4326));
      return new DataByteArray(polygon.asBinary().array());
    }
    return null;
//    throw new ExecException("Cannot connect a non-polygon shape");
  }

}
