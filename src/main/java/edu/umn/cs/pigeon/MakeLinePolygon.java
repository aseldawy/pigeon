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
import java.util.Iterator;

import org.apache.pig.EvalFunc;
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
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPolygon;

/**
 * Takes a list of point locations and IDs and creates either a linestring or
 * polygon based on whether the last point is the same as the first point or not.
 * @author Ahmed Eldawy
 */
public class MakeLinePolygon extends EvalFunc<DataByteArray>{
  
  private GeometryParser geometryParser = new GeometryParser();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    DataBag pointIDs = (DataBag) b.get(0);
    DataBag pointLocations = (DataBag) b.get(1);
    Point[] coordinates = new Point[(int) pointLocations.size()];
    int i = 0;
    Iterator<Tuple> iter_id = pointIDs.iterator();
    long first_point_id = -1;
    boolean is_polygon = false;
    for (Tuple t : pointLocations) {
      Object point_id_obj = iter_id.next().get(0);
      long point_id = point_id_obj instanceof Integer?
          (int) point_id_obj :
          (long) point_id_obj;
      if (i == 0) {
        first_point_id = point_id;
        System.out.println(t.get(0));
        coordinates[i++] =
            (Point) (geometryParser.parseGeom(t.get(0))).getEsriGeometry();
      } else if (i == pointIDs.size() - 1) {
        is_polygon = point_id == first_point_id;
        if (is_polygon)
          coordinates[i++] = coordinates[0];
        else
          coordinates[i++] =
            (Point) (geometryParser.parseGeom(t.get(0))).getEsriGeometry();
      } else {
        coordinates[i++] =
            (Point) (geometryParser.parseGeom(t.get(0))).getEsriGeometry();
      }
    }
    if (is_polygon && coordinates.length <= 3) {
      // Cannot create a polygon with two corners, convert to Linestring
      Point[] new_coords = new Point[coordinates.length - 1];
      System.arraycopy(coordinates, 0, new_coords, 0, new_coords.length);
      coordinates = new_coords;
      is_polygon = false;
    }
    MultiPath multi_path = is_polygon ? new Polygon() : new Polyline();
    // Iterate over all segments. Skip last segment for polygons because
    // it's redundant with first point
    for (i = 1; i < (is_polygon? coordinates.length - 1 : coordinates.length); i++) {
      Segment segment = new Line();
      segment.setStart(coordinates[i-1]);
      segment.setEnd(coordinates[i]);
      multi_path.addSegment(segment, false);
    }
    OGCGeometry linestring = is_polygon?
        new OGCPolygon((Polygon)multi_path, 0, SpatialReference.create(4326)) :
        new OGCLineString(multi_path, 0, SpatialReference.create(4326));
    return new DataByteArray(linestring.asBinary().array());
  }

}
