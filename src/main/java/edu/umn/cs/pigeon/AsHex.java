/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;

/**
 * Returns the Well-Known Binary (WKB) representation of a geometry object
 * represented as hex string.
 * @author Ahmed Eldawy
 *
 */
public class AsHex extends EvalFunc<String> {

  private ESRIGeometryParser geometryParser = new ESRIGeometryParser();
  
  @Override
  public String exec(Tuple t) throws IOException {
    if (t.size() != 1)
      throw new IOException("AsHex expects one geometry argument");
    OGCGeometry geom = geometryParser.parseGeom(t.get(0));
    return ESRIGeometryParser.bytesToHex(geom.asBinary().array());
  }

}
