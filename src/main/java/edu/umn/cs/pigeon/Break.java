/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.Vector;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Breaks down a linestring or a polygon into straight line segments.
 * The generated segments are returned as a bag of tuples with the common
 * schema (Segment_ID, x1, y1, x2, y2)
 * 
 * @author Ahmed Eldawy
 */
public class Break extends EvalFunc<DataBag>{
  
  private JTSGeometryParser geometryParser = new JTSGeometryParser();

  @Override
  public DataBag exec(Tuple b) throws IOException {
    if (b.size() != 1)
      throw new GeoException("Invalid number of arguments. Expected 1 but found "+b.size());
    Geometry geom = geometryParser.parseGeom(b.get(0));
    Vector<Coordinate[]> segments = new Vector<Coordinate[]>();
    breakGeom(geom, segments);
    DataBag segmentsBag = BagFactory.getInstance().newDefaultBag();
    for (int i = 0; i < segments.size(); i++) {
      Tuple segmentTuple = TupleFactory.getInstance().newTuple(5);
      segmentTuple.set(0, i);
      segmentTuple.set(1, segments.get(i)[0].x);
      segmentTuple.set(2, segments.get(i)[0].y);
      segmentTuple.set(3, segments.get(i)[1].x);
      segmentTuple.set(4, segments.get(i)[1].y);
      segmentsBag.add(segmentTuple);
    }
    return segmentsBag;
  }

  private void breakGeom(Geometry geom, Vector<Coordinate[]> segments) {
    if (geom == null)
      return;
    if (geom instanceof LineString) {
      LineString linestring = (LineString) geom;
      Coordinate[] coordinates = linestring.getCoordinates();
      for (int i = 1; i < coordinates.length; i++) {
        Coordinate[] segment = new Coordinate[2];
        segment[0] = new Coordinate(coordinates[i-1]);
        segment[1] = new Coordinate(coordinates[i]);
        segments.add(segment);
      }
    } else if (geom instanceof Polygon) {
      Polygon polygon = (Polygon) geom;
      breakGeom(polygon.getExteriorRing(), segments);
      for (int n = 0; n < polygon.getNumInteriorRing(); n++) {
        breakGeom(polygon.getInteriorRingN(n), segments);
      }
    } else if (geom instanceof GeometryCollection) {
      GeometryCollection geomCollection = (GeometryCollection) geom;
      for (int n = 0; n < geomCollection.getNumGeometries(); n++) {
        breakGeom(geomCollection.getGeometryN(n), segments);
      }
    } else if (geom instanceof Point) {
      // Skip
    } else {
      throw new RuntimeException("Cannot break geometry of type "+geom.getClass());
    }
  }
  
  public Schema outputSchema(Schema input) {
    try {
      Schema segmentSchema = new Schema();
      segmentSchema.add(new Schema.FieldSchema("position", DataType.INTEGER));
      segmentSchema.add(new Schema.FieldSchema("x1", DataType.DOUBLE));
      segmentSchema.add(new Schema.FieldSchema("y1", DataType.DOUBLE));
      segmentSchema.add(new Schema.FieldSchema("x2", DataType.DOUBLE));
      segmentSchema.add(new Schema.FieldSchema("y2", DataType.DOUBLE));

      FieldSchema breakSchema = new Schema.FieldSchema("segments", segmentSchema);
      breakSchema.type = DataType.BAG;
      
      return new Schema(breakSchema);
    } catch (Exception e) {
      return null;
    }
  }
}
