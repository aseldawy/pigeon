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

import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * A UDF that tests whether a geometry intersects another geometry or not
 * @author Tuan Pham
 *
 */
public class Intersects extends EvalFunc<Boolean> {
  
  private final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public Boolean exec(Tuple input) throws IOException {
    try {
      OGCGeometry geom1 = geometryParser.parseGeom(input.get(0));
      OGCGeometry geom2 = geometryParser.parseGeom(input.get(1));
      return geom1.intersects(geom2);
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
