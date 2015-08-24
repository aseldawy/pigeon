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

import com.esri.core.geometry.ogc.OGCGeometry;

/**
 * @author Carlos Balduz
 *
 */
public class ESRIShapeFromText extends EvalFunc<DataByteArray> {
    
  @Override
  public DataByteArray exec(Tuple input) throws IOException {
              
    if (input.size() != 1) 
        throw new IOException("ESRIShapeFromText takes one bytearray argument");

    String s = input.get(0).toString();
    return new DataByteArray(OGCGeometryToESRIShapeParser.ogcGeomToEsriShape(s)); 
  }
}
