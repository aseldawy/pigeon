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

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPoint;

/**
 * @author Ahmed Eldawy
 *
 */
public class MakePoint extends EvalFunc<DataByteArray> {
  
  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    if (input.size() != 2)
      throw new GeoException("MakePoint takes two numerical arguments");
    double x = ESRIGeometryParser.parseDouble(input.get(0));
    double y = ESRIGeometryParser.parseDouble(input.get(1));
    Point point = new Point(x, y);
    OGCPoint ogc_point = new OGCPoint(point, SpatialReference.create(4326));
    return new DataByteArray(ogc_point.asBinary().array());
  }
}
