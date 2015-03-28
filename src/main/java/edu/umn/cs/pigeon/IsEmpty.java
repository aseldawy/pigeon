/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * A predicate that tells whether a specific geometry is empty or not.
 * A wrapper call to {@link OGCGeometry#isEmpty()}
 * @author Ahmed Eldawy
 *
 */
public class IsEmpty extends FilterFunc {
  
  private final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public Boolean exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      OGCGeometry geom = geometryParser.parseGeom(v);
      return geom.isEmpty();
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
