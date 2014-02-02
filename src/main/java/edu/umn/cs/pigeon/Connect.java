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
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.Segment;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
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
      firstPointId.add((Long) t.get(0));
    }
    
    Vector<Long> lastPointId = new Vector<Long>();
    for (Tuple t : (DataBag) b.get(1)) {
      lastPointId.add((Long) t.get(0));
    }

    Vector<OGCLineString> linestrings = new Vector<OGCLineString>();
    for (Tuple t : (DataBag) b.get(2)) {
      linestrings.add((OGCLineString) geometryParser.parseGeom(t.get(0)));
    }
    
    if (firstPointId.size() != lastPointId.size() ||
        firstPointId.size() != linestrings.size()) {
      throw new ExecException("All parameters should be of the same size ("
          + firstPointId.size() + "," + lastPointId.size() + ","
          + linestrings.size() + ")");
    }

    // Shapes that are created after connected line segments
    Vector<OGCGeometry> createdShapes = new Vector<OGCGeometry>();
    // Stores an ordered list of line segments in current connected block
    Vector<OGCLineString> connected_lines = new Vector<OGCLineString>();
    // Total number of points in all visited linestrings
    int sumPoints = 0;
    // Which linestrings to reverse upon connection
    Vector<Boolean> reverse = new Vector<Boolean>();
    long first_point_id = -1;
    long last_point_id = -1;
    
    // Reorder linestrings to form a contiguous list of connected linestrings
    while (!linestrings.isEmpty()) {
      // Loop invariant:
      // At the beginning of each iteration, the lines in connected_lines are connected.
      // In each iteration, we move one linestring from linestrings to connected_lines
      // while keeping them connected
      int size_before = connected_lines.size();
      for (int i = 0; i < linestrings.size();) {
        if (connected_lines.isEmpty()) {
          // First linestring
          first_point_id = firstPointId.remove(i);
          last_point_id = lastPointId.remove(i);
          reverse.add(false);
          sumPoints += linestrings.get(i).numPoints();
          connected_lines.add(linestrings.remove(i));
        } else if (lastPointId.get(i) == first_point_id) {
          // This linestring goes to the beginning of the list as-is
          lastPointId.remove(i);
          first_point_id = firstPointId.remove(i);
          sumPoints += linestrings.get(i).numPoints();
          connected_lines.add(0, linestrings.remove(i));
          reverse.add(0, false);
        } else if (firstPointId.get(i) == first_point_id) {
          // Should go to the beginning after being reversed
          firstPointId.remove(i);
          first_point_id = lastPointId.remove(i);
          sumPoints += linestrings.get(i).numPoints();
          connected_lines.add(0, linestrings.remove(i));
          reverse.add(0, true);
        } else if (firstPointId.get(i) == last_point_id) {
          // This linestring goes to the end of the list as-is
          firstPointId.remove(i);
          last_point_id = lastPointId.remove(i);
          sumPoints += linestrings.get(i).numPoints();
          connected_lines.add(linestrings.remove(i));
          reverse.add(false);
        } else if (lastPointId.get(i) == last_point_id) {
          // Should go to the end after being reversed
          lastPointId.remove(i);
          last_point_id = firstPointId.remove(i);
          sumPoints += linestrings.get(i).numPoints();
          connected_lines.add(linestrings.remove(i));
          reverse.add(true);
        } else {
          i++;
        }
      }
      
      if (connected_lines.size() == size_before || linestrings.isEmpty()) {
        // Cannot connect any more lines to the current block. Emit as a shape
        // A polygon
        boolean isPolygon = first_point_id == last_point_id;
        Point[] points = new Point[sumPoints - connected_lines.size() + (isPolygon? 0 : 1)];
        int n = 0;
        for (int i = 0; i < connected_lines.size(); i++) {
          OGCLineString linestring = connected_lines.get(i);
          boolean isReverse = reverse.get(i);
          int last_i = (isPolygon || i < connected_lines.size() - 1)?
              linestring.numPoints() - 1 : linestring.numPoints();
          for (int i_point = 0; i_point < last_i; i_point++) {
            points[n++] = (Point) linestring.pointN(
                isReverse? linestring.numPoints() - 1 - i_point : i_point
                ).getEsriGeometry();
          }
        }
        
        MultiPath multi_path = isPolygon ? new Polygon() : new Polyline();
        for (int i = 1; i <points.length; i++) {
          Segment segment = new Line();
          segment.setStart(points[i-1]);
          segment.setEnd(points[i]);
          multi_path.addSegment(segment, false);
        }
        createdShapes.add(isPolygon ? new OGCPolygon((Polygon) multi_path, 0,
            SpatialReference.create(4326)) : new OGCLineString(
            (Polyline) multi_path, 0, SpatialReference.create(4326)));

        // Re-initialize all data structures to connect remaining lines
        if (!linestrings.isEmpty()) {
          connected_lines.clear();
          reverse.clear();
          sumPoints = 0;
        }
      }
    }

    if (createdShapes.size() == 1) {
      return new DataByteArray(createdShapes.get(0).asBinary().array());
    } else if (createdShapes.size() > 1) {
      OGCGeometryCollection collection = new OGCConcreteGeometryCollection(createdShapes, createdShapes.get(0).getEsriSpatialReference());
      return new DataByteArray(collection.asBinary().array());
    } else {
      throw new ExecException("Cannot connect a non-polygon shape");
    }
  }

}
