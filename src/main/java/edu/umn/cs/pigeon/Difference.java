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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * A UDF that returns the spatial difference of two shapes as calculated by
 * {@link OGCGeometry#difference()}
 * @author Ahmed Eldawy
 *
 */
public class Difference extends EvalFunc<DataByteArray> {
  
  private final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    try {
      OGCGeometry geom1 = geometryParser.parseGeom(input.get(0));
      OGCGeometry geom2 = geometryParser.parseGeom(input.get(1));
      return new DataByteArray(geom1.difference(geom2).asBinary().array());
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
