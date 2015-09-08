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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * A UDF to create a box (rectangle) from the coordinates of the two corners
 * x1, y1, x2, and y2.
 * @author Ahmed Eldawy
 *
 */
public class MakeBox extends EvalFunc<DataByteArray> {
  
  private GeometryFactory geometryFactory = new GeometryFactory();
  private WKBWriter wkbWriter = new WKBWriter();
  
  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    if (input.size() != 4)
      throw new IOException("MakeBox takes four numerical arguments");
    double x1 = ESRIGeometryParser.parseDouble(input.get(0));
    double y1 = ESRIGeometryParser.parseDouble(input.get(1));
    double x2 = ESRIGeometryParser.parseDouble(input.get(2));
    double y2 = ESRIGeometryParser.parseDouble(input.get(3));
    Coordinate[] corners = new Coordinate[5];
    corners[0] = new Coordinate(x1, y1);
    corners[1] = new Coordinate(x1, y2);
    corners[2] = new Coordinate(x2, y2);
    corners[3] = new Coordinate(x2, y1);
    corners[4] = corners[0];
    
    Polygon box = geometryFactory.createPolygon(geometryFactory.createLinearRing(corners), null);

    return new DataByteArray(wkbWriter.write(box));
  }
}
