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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


/**
 * Returns the size of a geometry in terms of number of points.
 * For {@link Point} it always returns one.
 * For {@link LineString} it returns number of points.
 * For {@link Polygon} it returns number of edges.
 * @author Ahmed Eldawy
 *
 */
public class NumPoints extends EvalFunc<Integer> {
  
  private final JTSGeometryParser geometryParser = new JTSGeometryParser();

  @Override
  public Integer exec(Tuple input) throws IOException {
    Geometry geom = null;
    try {
      Object v = input.get(0);
      geom = geometryParser.parseGeom(v);
      return getGeometrySize(geom);
    } catch (ExecException ee) {
      throw new GeoException(geom, ee);
    }
  }
  
  protected static int getGeometrySize(Geometry geom) throws ExecException {
    if (geom instanceof Point) {
      return 1;
    } else if (geom instanceof LineString) {
      return ((LineString)geom).getNumPoints();
    } else if (geom instanceof Polygon) {
      Polygon poly = (Polygon) geom;
      int size = 0;
      size += getGeometrySize(poly.getExteriorRing()) - 1;
      for (int i = 0; i < poly.getNumInteriorRing(); i++)
        size += getGeometrySize(poly.getInteriorRingN(i)) - 1;
      return size;
    } else if (geom instanceof GeometryCollection) {
      int size = 0;
      GeometryCollection coll = (GeometryCollection) geom;
      for (int i = 0; i < coll.getNumGeometries(); i++)
        size += getGeometrySize(coll.getGeometryN(i));
      return size;
    } else {
      throw new GeoException("size() not defined for shapes of type: "+geom.getClass());
    }
  }

}
