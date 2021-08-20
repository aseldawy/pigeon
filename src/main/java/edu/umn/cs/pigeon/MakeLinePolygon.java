/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

/**
 * Takes a list of point locations and IDs and creates either a linestring or
 * polygon based on whether the last point is the same as the first point or not.
 * @author Ahmed Eldawy
 */
public class MakeLinePolygon extends EvalFunc<DataByteArray>{
  
  private GeometryFactory geometryFactory = new GeometryFactory();
  private JTSGeometryParser geometryParser = new JTSGeometryParser();
  private WKBWriter wkbWriter = new WKBWriter();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    DataBag pointIDs = (DataBag) b.get(0);
    DataBag pointLocations = (DataBag) b.get(1);
    Coordinate[] coordinates = new Coordinate[(int) pointLocations.size()];
    int i = 0;
    Iterator<Tuple> iter_id = pointIDs.iterator();
    long first_point_id = -1;
    boolean is_polygon = false;
    for (Tuple t : pointLocations) {
      Object point_id_obj = iter_id.next().get(0);
      Geometry point = geometryParser.parseGeom(t.get(0));
      long point_id = point_id_obj instanceof Integer?
          (Integer) point_id_obj :
          (Long) point_id_obj;
      if (i == 0) {
        first_point_id = point_id;
        coordinates[i++] = point.getCoordinate();
      } else if (i == pointIDs.size() - 1) {
        is_polygon = point_id == first_point_id;
        if (is_polygon)
          coordinates[i++] = coordinates[0];
        else
          coordinates[i++] = point.getCoordinate();
      } else {
        coordinates[i++] = point.getCoordinate();
      }
    }
    Geometry shape;
    if (coordinates.length == 1 || (coordinates.length == 2 && is_polygon)) {
      // A point
      shape = geometryFactory.createPoint(coordinates[0]);
    } else {
      if (is_polygon && coordinates.length <= 3) {
        // Cannot create a polygon with two corners, convert to Linestring
        Coordinate[] new_coords = new Coordinate[coordinates.length - 1];
        System.arraycopy(coordinates, 0, new_coords, 0, new_coords.length);
        coordinates = new_coords;
        is_polygon = false;
      }
      if  (is_polygon) {
        shape = geometryFactory.createPolygon(geometryFactory.createLinearRing(coordinates), null);
      } else {
        shape = geometryFactory.createLineString(coordinates);
      }
    }
    return new DataByteArray(wkbWriter.write(shape));
  }
}
