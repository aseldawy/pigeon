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
 * A UDF that returns the minimal bounding rectangle (MBR) of a shape.
 * @author Ahmed Eldawy
 *
 */
public class Buffer extends EvalFunc<DataByteArray> {
  
  private final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    OGCGeometry geom = null;
    try {
      Object v = input.get(0);
      geom = geometryParser.parseGeom(v);
      double dist;
      Object distance = input.get(1);
      if (distance instanceof Double)
        dist = (Double) distance;
      else if (distance instanceof Float)
        dist = (Float) distance;
      else if (distance instanceof Integer)
        dist = (Integer) distance;
      else if (distance instanceof Long)
        dist = (Long) distance;
      else if (distance instanceof String)
        dist = Double.parseDouble((String) distance);
      else if (distance instanceof DataByteArray)
        dist = Double.parseDouble(new String(((DataByteArray) distance).get()));
      else
        throw new GeoException("Invalid second argument in call to Buffer. Expecting Double, Integer or Long");
      return new DataByteArray(geom.buffer(dist).asBinary().array());
    } catch (ExecException ee) {
      throw new GeoException(geom, ee);
    }
  }

}
