/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

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
 * Checks which grid cells overlap a geometric shape based on its MBR
 * 
 * @author Ahmed Eldawy
 */
public class GridPartition extends EvalFunc<DataBag> {
  
  private final JTSGeometryParser geometryParser = new JTSGeometryParser();

  @Override
  public DataBag exec(Tuple b) throws IOException {
    Geometry geomMBR = geometryParser.parseGeom(b.get(0)).getEnvelope();
    Geometry gridMBR = geometryParser.parseGeom(b.get(1)).getEnvelope();
    int gridSize = (Integer)b.get(2);

    DataBag output = BagFactory.getInstance().newDefaultBag();

    Coordinate[] gridCoords = gridMBR.getCoordinates();
    double gridX1 = Math.min(gridCoords[0].x, gridCoords[2].x);
    double gridY1 = Math.min(gridCoords[0].y, gridCoords[2].y);
    double gridX2 = Math.max(gridCoords[0].x, gridCoords[2].x);
    double gridY2 = Math.max(gridCoords[0].y, gridCoords[2].y);

    Coordinate[] geomCoords = geomMBR.getCoordinates();
    double geomX1, geomY1, geomX2, geomY2;
    if (geomCoords.length == 1) {
      // A special case for point
      geomX1 = geomX2 = geomCoords[0].x;
      geomY1 = geomY2 = geomCoords[0].y;
    } else {
      geomX1 = Math.min(geomCoords[0].x, geomCoords[2].x);
      geomY1 = Math.min(geomCoords[0].y, geomCoords[2].y);
      geomX2 = Math.max(geomCoords[0].x, geomCoords[2].x);
      geomY2 = Math.max(geomCoords[0].y, geomCoords[2].y);
    }

    
    int col1 = (int) (Math.floor(geomX1 - gridX1) * gridSize / (gridX2 - gridX1));
    int row1 = (int) (Math.floor(geomY1 - gridY1) * gridSize / (gridY2 - gridY1));
    int col2 = (int) (Math.ceil(geomX2 - gridX1) * gridSize / (gridX2 - gridX1));
    int row2 = (int) (Math.ceil(geomY2 - gridY1) * gridSize / (gridY2 - gridY1));
    
    for (int col = col1; col < col2; col++) {
      for (int row = row1; row < row2; row++) {
        int cellID = row * gridSize + col;
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, cellID);
        output.add(tuple);
      }
    }
    return output;
  }

  public Schema outputSchema(Schema input) {
    try {
      Schema partSchema = new Schema();
      partSchema.add(new Schema.FieldSchema("cellID", DataType.INTEGER));

      FieldSchema outSchema = new Schema.FieldSchema("overlapCells", partSchema);
      outSchema.type = DataType.BAG;
      
      return new Schema(outSchema);
    } catch (Exception e) {
      return null;
    }
  }
}


