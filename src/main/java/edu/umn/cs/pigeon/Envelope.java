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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;


/**
 * A UDF that returns the minimal bounding rectangle (MBR) of a shape.
 * @author Ahmed Eldawy
 *
 */
public class Envelope extends EvalFunc<DataByteArray> {
  
  private final WKBWriter WKB_WRITER = new WKBWriter();
  private final JTSGeometryParser GEOMETRY_PARSER = new JTSGeometryParser();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    Geometry geom = null;
    try {
      Object v = input.get(0);
      geom = GEOMETRY_PARSER.parseGeom(v);
      Geometry envelope = geom.getEnvelope();
      return new DataByteArray(WKB_WRITER.write(envelope));
    } catch (ExecException ee) {
      throw new GeoException(geom, ee);
    }
  }

}
