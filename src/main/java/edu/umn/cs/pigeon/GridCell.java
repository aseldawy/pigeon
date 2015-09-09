/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * Returns the boundaries of one cell
 * GridPartition(cellid, gridMBR, gridSize)
 * where:
 * cellid: the ID of the cell to retrieve.
 * gridMBR: the rectangle that defines the boundaries of the grid
 * gridSize: number of rows and columns assuming a uniform grid
 * @author Ahmed Eldawy
 */
public class GridCell extends EvalFunc<DataByteArray> {
  
  private final JTSGeometryParser geometryParser = new JTSGeometryParser();
  private GeometryFactory geometryFactory = new GeometryFactory();
  private WKBWriter wkbWriter = new WKBWriter();

  @Override
  public DataByteArray exec(Tuple b) throws IOException {
    int cellID = (Integer) b.get(0);
    Geometry gridMBR = geometryParser.parseGeom(b.get(1)).getEnvelope();
    int gridSize = (Integer)b.get(2);

    Coordinate[] gridCoords = gridMBR.getCoordinates();
    double gridX1 = Math.min(gridCoords[0].x, gridCoords[2].x);
    double gridY1 = Math.min(gridCoords[0].y, gridCoords[2].y);
    double gridX2 = Math.max(gridCoords[0].x, gridCoords[2].x);
    double gridY2 = Math.max(gridCoords[0].y, gridCoords[2].y);

    int column = cellID % gridSize;
    int row = cellID / gridSize;
    double cellX1 = column * (gridX2 - gridX1) / gridSize + gridX1;
    double cellX2 = (column + 1) * (gridX2 - gridX1) / gridSize + gridX1;
    double cellY1 = row * (gridY2 - gridY1) / gridSize + gridY1;
    double cellY2 = (row + 1) * (gridY2 - gridY1) / gridSize + gridY1;
    
    Coordinate[] corners = new Coordinate[5];
    corners[0] = new Coordinate(cellX1, cellY1);
    corners[1] = new Coordinate(cellX1, cellY2);
    corners[2] = new Coordinate(cellX2, cellY2);
    corners[3] = new Coordinate(cellX2, cellY1);
    corners[4] = corners[0];
    
    Polygon box = geometryFactory.createPolygon(geometryFactory.createLinearRing(corners), null);

    return new DataByteArray(wkbWriter.write(box));
  }

}


