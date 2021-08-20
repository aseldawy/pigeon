/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;


/**
 * A UDF that returns the left bound of the object
 * @author Ahmed Eldawy
 *
 */
public class YMin extends EvalFunc<Double> {
  
  private final JTSGeometryParser geometryParser = new JTSGeometryParser();

  @Override
  public Double exec(Tuple input) throws IOException {
    Geometry geom = null;
    try {
      Object v = input.get(0);
      geom = geometryParser.parseGeom(v);
      Coordinate[] coords = geom.getEnvelope().getCoordinates();
      if (coords.length == 0)
        throw new ExecException("YMin cannot work on empty geometires");
      if (coords.length == 1)
        return coords[0].y;
      if (coords.length == 2)
        return Math.min(coords[0].y, coords[1].y);
      return Math.min(coords[0].y, coords[2].y);
    } catch (ExecException ee) {
      throw new GeoException(geom, ee);
    }
  }

}
