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
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Takes a list of point locations and IDs and creates either a linestring or
 * polygon based on whether the last point is the same as the first point or not.
 * @author Ahmed Eldawy
 */
public class MakeSegments extends EvalFunc<DataBag>{
  
  private JTSGeometryParser geometryParser = new JTSGeometryParser();

  @Override
  public DataBag exec(Tuple b) throws IOException {
    DataBag pointIDs = (DataBag) b.get(0);
    DataBag pointLocations = (DataBag) b.get(1);
    long[] ids = new long[(int) pointIDs.size()];
    Coordinate[] coordinates = new Coordinate[(int) pointLocations.size()];
    int i = 0;
    Iterator<Tuple> iter_id = pointIDs.iterator();
    for (Tuple t : pointLocations) {
      Object point_id_obj = iter_id.next().get(0);
      Geometry point = geometryParser.parseGeom(t.get(0));
      long point_id = point_id_obj instanceof Integer?
          (Integer) point_id_obj :
          (Long) point_id_obj;
      ids[i] = point_id;
      coordinates[i++] = point.getCoordinate();
    }
    
    DataBag segmentsBag = BagFactory.getInstance().newDefaultBag();
    for (int n = 1; n < coordinates.length; n++) {
      Tuple segmentTuple = TupleFactory.getInstance().newTuple(7);
      segmentTuple.set(0, n - 1);
      segmentTuple.set(1, ids[n-1]);
      segmentTuple.set(2, coordinates[n-1].x);
      segmentTuple.set(3, coordinates[n-1].y);
      segmentTuple.set(4, ids[n]);
      segmentTuple.set(5, coordinates[n].x);
      segmentTuple.set(6, coordinates[n].y);
      
      segmentsBag.add(segmentTuple);
    }
    return segmentsBag;
  }
  
  public Schema outputSchema(Schema input) {
    try {
      Schema segmentSchema = new Schema();
      segmentSchema.add(new Schema.FieldSchema("position", DataType.INTEGER));
      segmentSchema.add(new Schema.FieldSchema("id1", DataType.LONG));
      segmentSchema.add(new Schema.FieldSchema("x1", DataType.DOUBLE));
      segmentSchema.add(new Schema.FieldSchema("y1", DataType.DOUBLE));
      segmentSchema.add(new Schema.FieldSchema("id2", DataType.LONG));
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
