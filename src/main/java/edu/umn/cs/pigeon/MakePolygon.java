/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.Line;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Segment;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPolygon;

/**
 * Generates a geometry of type Polygon out of a bag of points. The last point
 * in the bag should be the same as the first point.
 * @author Ahmed Eldawy
 */
public class MakePolygon extends EvalFunc<DataByteArray>{
  
  private ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    DataBag points = (DataBag) b.get(0);
    Point[] coordinates = new Point[(int) points.size()];
    int i = 0;
    for (Tuple t : points) {
      coordinates[i++] =
          (Point) (geometryParser.parseGeom(t.get(0))).getEsriGeometry();
    }
    Polygon multi_path = new Polygon();
    for (i = 1; i <coordinates.length; i++) {
      Segment segment = new Line();
      segment.setStart(coordinates[i-1]);
      segment.setEnd(coordinates[i]);
      multi_path.addSegment(segment, false);
    }
    OGCPolygon linestring = new OGCPolygon(multi_path, 0,
        SpatialReference.create(4326));
    return new DataByteArray(linestring.asBinary().array());
  }

}
