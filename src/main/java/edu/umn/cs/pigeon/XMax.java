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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;


/**
 * A UDF that returns the left bound of the object
 * @author Ahmed Eldawy
 *
 */
public class XMax extends EvalFunc<Double> {
  
  private final JTSGeometryParser geometryParser = new JTSGeometryParser();

  @Override
  public Double exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      Geometry geom = geometryParser.parseGeom(v);
      Coordinate[] coords = geom.getEnvelope().getCoordinates();
      return Math.max(coords[0].x, Math.max(coords[1].x, coords[2].x));
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
