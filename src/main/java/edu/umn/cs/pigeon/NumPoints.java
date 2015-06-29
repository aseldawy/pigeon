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
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;


/**
 * Returns the size of a geometry in terms of number of points.
 * For {@link OGCPoint} it always returns one.
 * For {@link OGCLineString} it returns number of points.
 * For {@link OGCPolygon} it returns number of edges.
 * @author Ahmed Eldawy
 *
 */
public class NumPoints extends EvalFunc<Integer> {
  
  private final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public Integer exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      OGCGeometry geom = geometryParser.parseGeom(v);
      return getGeometrySize(geom);
    } catch (ExecException ee) {
      throw ee;
    }
  }
  
  private int getGeometrySize(OGCGeometry geom) throws ExecException {
    if (geom instanceof OGCPoint) {
      return 1;
    } else if (geom instanceof OGCLineString) {
      return ((OGCLineString)geom).numPoints();
    } else if (geom instanceof OGCPolygon) {
      OGCPolygon poly = (OGCPolygon) geom;
      int size = 0;
      size += getGeometrySize(poly.exteriorRing()) - 1;
      for (int i = 0; i < poly.numInteriorRing(); i++)
        size += getGeometrySize(poly.interiorRingN(i)) - 1;
      return size;
    } else if (geom instanceof OGCGeometryCollection) {
      int size = 0;
      OGCGeometryCollection coll = (OGCGeometryCollection) geom;
      for (int i = 0; i < coll.numGeometries(); i++)
        size += getGeometrySize(coll.geometryN(i));
      return size;
    } else {
      throw new ExecException("size() not defined for shapes of type: "+geom.getClass());
    }
  }

}
