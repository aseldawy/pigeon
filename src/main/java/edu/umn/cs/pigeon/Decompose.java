/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.Stack;
import java.util.Vector;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;

/**
 * Decomposes a polygon into smaller polygons using a Quad-tree-based
 * decomposition
 * 
 * @author Ahmed Eldawy
 */
public class Decompose extends EvalFunc<DataBag> {
  
  /**Default size threshold*/
  public static final int DefaultThreshold = 100;
  
  private final WKBWriter wkbWriter = new WKBWriter();
  
  private JTSGeometryParser geometryParser = new JTSGeometryParser();
  
  /**Geometry factory to create smaller geometries*/
  private GeometryFactory geomFactory = new GeometryFactory();

  @Override
  public DataBag exec(Tuple b) throws IOException {
    Geometry geom = geometryParser.parseGeom(b.get(0));
    int threshold = b.size() == 1? DefaultThreshold : (Integer)b.get(1);
    if (threshold < 4)
      throw new GeoException("Size threshold must be at least 4");
    DataBag output = BagFactory.getInstance().newDefaultBag();
    Stack<Geometry> toDecompose = new Stack<Geometry>();
    toDecompose.push(geom);
    while (!toDecompose.isEmpty()) {
      geom = toDecompose.pop();
      if (NumPoints.getGeometrySize(geom) <= threshold) {
        // Simple enough. Add to the output
        DataByteArray data = new DataByteArray(wkbWriter.write(geom));
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, data);
        output.add(tuple);
      } else {
        // Large geometry. Decompose into four
        Geometry[] parts = decompose(geom);
        for (Geometry part : parts)
          if (!part.isEmpty())
            toDecompose.push(part);
      }
    }
    return output;
  }

  /**
   * Decomposes a geometry into smaller one.
   * @param geom
   * @return
   */
  public Geometry[] decompose(Geometry geom) {
    if (geom instanceof GeometryCollection) {
      GeometryCollection coll = (GeometryCollection) geom;
      Vector<Geometry> output = new Vector<Geometry>();
      for (int i = 0; i < coll.getNumGeometries(); i++) {
        geom = coll.getGeometryN(i);
        Geometry[] partialAnswer = decompose(geom);
        for (Geometry g : partialAnswer)
          if (!g.isEmpty())
            output.add(g);
      }
      return output.toArray(new Geometry[output.size()]);
    }
    
    Geometry[] parts = new Geometry[4];
    Geometry envelope = geom.getEnvelope();
    Coordinate[] coords = envelope.getCoordinates();
    Coordinate[][] corners = new Coordinate[3][3];
    double x1 = Math.min(coords[0].x, coords[2].x);
    double x2 = Math.max(coords[0].x, coords[2].x);
    double y1 = Math.min(coords[0].y, coords[2].y);
    double y2 = Math.max(coords[0].y, coords[2].y);
    double cx = (x1 + x2) / 2;
    double cy = (y1 + y2) / 2;
    corners[0][0] = new Coordinate(x1, y1);
    corners[0][1] = new Coordinate(x1, cy);
    corners[0][2] = new Coordinate(x1, y2);
    corners[1][0] = new Coordinate(cx, y1);
    corners[1][1] = new Coordinate(cx, cy);
    corners[1][2] = new Coordinate(cx, y2);
    corners[2][0] = new Coordinate(x2, y1);
    corners[2][1] = new Coordinate(x2, cy);
    corners[2][2] = new Coordinate(x2, y2);
    
    Polygon q0 = geomFactory
        .createPolygon(
            geomFactory.createLinearRing(new Coordinate[] { corners[0][0],
                corners[1][0], corners[1][1], corners[0][1], corners[0][0] }),
            null);
    parts[0] = q0.intersection(geom);
    Polygon q1 = geomFactory
        .createPolygon(
            geomFactory.createLinearRing(new Coordinate[] { corners[0][1],
                corners[1][1], corners[1][2], corners[0][2], corners[0][1]}),
                null);
    parts[1] = q1.intersection(geom);
    Polygon q2 = geomFactory
        .createPolygon(
            geomFactory.createLinearRing(new Coordinate[] { corners[1][0],
                corners[2][0], corners[2][1], corners[1][1], corners[1][0] }),
                null);
    parts[2] = q2.intersection(geom);
    Polygon q3 = geomFactory
        .createPolygon(
            geomFactory.createLinearRing(new Coordinate[] { corners[1][1],
                corners[2][1], corners[2][2], corners[1][2], corners[1][1] }),
                null);
    parts[3] = q3.intersection(geom);
    
    return parts;
  }
  
  public Schema outputSchema(Schema input) {
    try {
      Schema partSchema = new Schema();
      partSchema.add(new Schema.FieldSchema("geom", DataType.BYTEARRAY));

      FieldSchema outSchema = new Schema.FieldSchema("geometries", partSchema);
      outSchema.type = DataType.BAG;
      
      return new Schema(outSchema);
    } catch (Exception e) {
      return null;
    }
  }
}

