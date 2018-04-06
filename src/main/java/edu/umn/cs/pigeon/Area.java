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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * A UDF that returns the area of a geometry as calculated by
 * {@link Geometry#calculateArea2D()}
 * @author Ahmed Eldawy
 *
 */
public class Area extends EvalFunc<Double> {
  
  private final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public Double exec(Tuple input) throws IOException {
    OGCGeometry geom = null;
    try {
      Object v = input.get(0);
      geom = geometryParser.parseGeom(v);
      return geom.getEsriGeometry().calculateArea2D();
    } catch (ExecException ee) {
      throw new GeoException(geom, ee);
    }
  }

}
