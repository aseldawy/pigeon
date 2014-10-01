/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * Generates a geometry of type LineString out of a bag of points.
 * @author Ahmed Eldawy
 */
public class MakeLine extends EvalFunc<DataByteArray>{
  
  private GeometryFactory geometryFactory = new GeometryFactory();
  private JTSGeometryParser geometryParser = new JTSGeometryParser();
  private WKBWriter wkbWriter = new WKBWriter();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    DataBag points = (DataBag) b.get(0);
    Coordinate[] coordinates = new Coordinate[(int) points.size()];
    int i = 0;
    for (Tuple t : points) {
      try {
        Geometry point = geometryParser.parseGeom(t.get(0));
        coordinates[i++] = point.getCoordinate();
      } catch (ParseException e) {
        throw new IOException("Error parsing "+t.get(0), e);
      }
    }
    Geometry line = geometryFactory.createLineString(coordinates);
    return new DataByteArray(wkbWriter.write(line));
  }

}
