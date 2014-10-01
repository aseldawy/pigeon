/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.Line;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Segment;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPolygon;

/**
 * A UDF to create a box (rectangle) from the coordinates of the two corners
 * x1, y1, x2, and y2.
 * @author Ahmed Eldawy
 *
 */
public class MakeBox extends EvalFunc<DataByteArray> {
  
  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    if (input.size() != 4)
      throw new IOException("MakeBox takes four numerical arguments");
    double x1 = ESRIGeometryParser.parseDouble(input.get(0));
    double y1 = ESRIGeometryParser.parseDouble(input.get(1));
    double x2 = ESRIGeometryParser.parseDouble(input.get(2));
    double y2 = ESRIGeometryParser.parseDouble(input.get(3));
    Point[] corners = new Point[5];
    corners[0] = new Point(x1, y1);
    corners[1] = new Point(x1, y2);
    corners[2] = new Point(x2, y2);
    corners[3] = new Point(x2, y1);
    corners[4] = corners[0];
    
    Polygon multi_path = new Polygon();
    for (int i = 1; i <corners.length; i++) {
      Segment segment = new Line();
      segment.setStart(corners[i-1]);
      segment.setEnd(corners[i]);
      multi_path.addSegment(segment, false);
    }
    OGCPolygon linestring = new OGCPolygon(multi_path, 0,
        SpatialReference.create(4326));

    return new DataByteArray(linestring.asBinary().array());
  }
}
