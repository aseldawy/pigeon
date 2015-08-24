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

/**
 * @author Carlos Balduz
 *
 */
public class ESRIShapeFromWKB extends EvalFunc<DataByteArray> {
    
  @Override
  public DataByteArray exec(Tuple input) throws IOException {
              
    if (input.size() != 1) 
        throw new IOException("ESRIShapeFromWKB takes one bytearray argument");

    Object o = input.get(0);
    return new DataByteArray(OGCGeometryToESRIShapeParser.ogcGeomToEsriShape(o)); 
  }
}
