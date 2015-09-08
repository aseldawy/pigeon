/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
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
 * Performs a spatial join using the plane-sweep algorithm.
 * General usage
 * SJPlaneSweep(dataset1, dataset2, duplicate-avoidance-rectangle,
 *   column-index1, column-index2)
 * dataset1: The left dataset
 * dataset2: The right dataset
 * duplicate-avoidance-rectangle: The rectangle to use to perform duplicate
 *   avoidance. If not set, no duplicate avoidance is carried out.
 * column-index1: The index (position) of the geometric column in dataset1
 * column-index2: The index (position) of the geometric column in dataset2
 * @author Ahmed Eldawy
 *
 */
public class SJPlaneSweep extends EvalFunc<DataBag> {

  private final JTSGeometryParser geomParser = new JTSGeometryParser();
  private TupleFactory tupleFactory;
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    DataBag left = (DataBag) input.get(0);
    DataBag right = (DataBag) input.get(1);
    
    boolean dupAvoidance = false;
    double mbrX1 = 0, mbrY1 = 0, mbrX2 = 0, mbrY2 = 0;
    if (input.size() > 2) {
      // Implement duplicate avoidance based on the MBR as specified by
      // the third argument
      Geometry cellMBR = geomParser.parseGeom(input.get(2));
      if (cellMBR != null) {
        dupAvoidance = true;
        Coordinate[] mbrCoords = cellMBR.getCoordinates();
        mbrX1 = Math.min(mbrCoords[0].x, mbrCoords[2].x);
        mbrY1 = Math.min(mbrCoords[0].y, mbrCoords[2].y);
        mbrX2 = Math.max(mbrCoords[0].x, mbrCoords[2].x);
        mbrY2 = Math.max(mbrCoords[0].y, mbrCoords[2].y);
      }
    }
    // Retrieve the index of the geometry column in each bag
    // -1 indicates that it is not specified by user and will be auto-detected
    int lGeomColumn = input.size() > 3? (Integer) input.get(3) : -1;
    int rGeomColumn = input.size() > 4? (Integer) input.get(4) : -1;
    
    // TODO ensure that the left bag is the smaller one for efficiency
    if (left.size() > Integer.MAX_VALUE)
      throw new ExecException("Size of left dataset is too large "+left.size());
    
    // Read all of the left dataset in memory
    final Tuple[] lTuples = new Tuple[(int) left.size()];
    int leftSize = 0;
    tupleFactory = TupleFactory.getInstance();
    for (Tuple t : left) {
      lTuples[leftSize++] = tupleFactory.newTupleNoCopy(t.getAll());
    }
    
    if (lGeomColumn == -1)
      lGeomColumn = detectGeomColumn(lTuples[0]);
    
    // Extract MBRs of objects for filter-refine approach
    final double[] lx1 = new double[(int) left.size()];
    final double[] ly1 = new double[(int) left.size()];
    final double[] lx2 = new double[(int) left.size()];
    final double[] ly2 = new double[(int) left.size()];
    for (int i = 0; i < lTuples.length; i++) {
      Geometry geom = geomParser.parseGeom(lTuples[i].get(lGeomColumn));
      Coordinate[] mbrCoords = geom.getEnvelope().getCoordinates();
      lx1[i] = Math.min(mbrCoords[0].x, mbrCoords[2].x);
      ly1[i] = Math.min(mbrCoords[0].y, mbrCoords[2].y);
      lx2[i] = Math.max(mbrCoords[0].x, mbrCoords[2].x);
      ly2[i] = Math.max(mbrCoords[0].y, mbrCoords[2].y);
    }
    // Sort left MBRs by x to prepare for the plane-sweep algorithm
    IndexedSortable lSortable = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Tuple tt = lTuples[i]; lTuples[i] = lTuples[j]; lTuples[j] = tt;
        double td = lx1[i]; lx1[i] = lx1[j]; lx1[j] = td;
        td = ly1[i]; ly1[i] = ly1[j]; ly1[j] = td;
        td = lx2[i]; lx2[i] = lx2[j]; lx2[j] = td;
        td = ly2[i]; ly2[i] = ly2[j]; ly2[j] = td;
      }
      
      @Override
      public int compare(int i, int j) {
        if (lx1[i] < lx1[j])
          return -1;
        if (lx2[i] > lx2[j])
          return 1;
        return 0;
      }
    };
    QuickSort quickSort = new QuickSort();
    quickSort.sort(lSortable, 0, lTuples.length);
    
    // Retrieve objects from the right relation in batches and join with left
    Iterator<Tuple> ri = right.iterator();
    final int batchSize = 10000;
    final Tuple[] rTuples = new Tuple[batchSize];
    final double[] rx1 = new double[batchSize];
    final double[] ry1 = new double[batchSize];
    final double[] rx2 = new double[batchSize];
    final double[] ry2 = new double[batchSize];
    IndexedSortable rSortable = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Tuple tt = rTuples[i]; rTuples[i] = rTuples[j]; rTuples[j] = tt;
        double td = rx1[i]; rx1[i] = rx1[j]; rx1[j] = td;
        td = ry1[i]; ry1[i] = ry1[j]; ry1[j] = td;
        td = rx2[i]; rx2[i] = rx2[j]; rx2[j] = td;
        td = ry2[i]; ry2[i] = ry2[j]; ry2[j] = td;
      }
      
      @Override
      public int compare(int i, int j) {
        if (rx1[i] < rx1[j]) return -1;
        if (rx2[i] > rx2[j]) return 1;
        return 0;
      }
    };
    int rSize = 0;
    DataBag output = BagFactory.getInstance().newDefaultBag();
    while (ri.hasNext()) {
      rTuples[rSize++] = tupleFactory.newTupleNoCopy(ri.next().getAll());
      if (rSize == batchSize || !ri.hasNext()) {
        // Extract MBRs of geometries on the right
        if (rGeomColumn == -1)
          rGeomColumn = detectGeomColumn(rTuples[0]);
        
        for (int i = 0; i < rSize; i++) {
          Geometry geom = geomParser.parseGeom(rTuples[i].get(rGeomColumn));
          Coordinate[] mbrCoords = geom.getEnvelope().getCoordinates();
          rx1[i] = Math.min(mbrCoords[0].x, mbrCoords[2].x);
          ry1[i] = Math.min(mbrCoords[0].y, mbrCoords[2].y);
          rx2[i] = Math.max(mbrCoords[0].x, mbrCoords[2].x);
          ry2[i] = Math.max(mbrCoords[0].y, mbrCoords[2].y);
        }
        
        // Perform the join now
        quickSort.sort(rSortable, 0, rSize);
        int i = 0, j = 0;

        while (i < lTuples.length && j < rSize) {
          if (lx1[i] < rx1[j]) {
            int jj = j;
            // Compare left object i to all right object jj
            while (jj < rSize && rx1[jj] <= lx2[i]) {
              if (lx2[i] > rx1[jj] && rx2[jj] > lx1[i] &&
                  ly2[i] > ry1[jj] && ry2[jj] > ly1[i]) {
                boolean report = true;
                if (dupAvoidance) {
                  // Performs the reference point technique to avoid duplicates
                  double intersectX = Math.max(lx1[i], rx1[jj]);
                  if (intersectX >= mbrX1 && intersectX < mbrX2) {
                    double intersectY = Math.max(ly1[i], ry1[jj]);
                    report = intersectY >= mbrY1 && intersectY < mbrY2;
                  } else {
                    report = false;
                  }
                }
                if (report) {
//                  Geometry lGeom = geomParser.parseGeom(lTuples[i].get(lGeomColumn));
//                  Geometry rGeom = geomParser.parseGeom(rTuples[jj].get(rGeomColumn));
//                  if (lGeom.overlaps(rGeom))
                    addToAnswer(output, lTuples[i], rTuples[jj]);
                }
              }
              jj++;
              progress();
            }
            i++;
          } else {
            int ii = i;
            // Compare all left objects ii to the right object j
            while (ii < lTuples.length && lx1[ii] <= rx2[j]) {
              if (lx2[ii] > rx1[j] && rx2[j] > lx1[ii] &&
                  ly2[ii] > ry1[j] && ry2[j] > ly1[ii]) {
                boolean report = true;
                if (dupAvoidance) {
                  // Performs the reference point technique to avoid duplicates
                  double intersectX = Math.max(lx1[ii], rx1[j]);
                  if (intersectX >= mbrX1 && intersectX < mbrX2) {
                    double intersectY = Math.max(ly1[ii], ry1[j]);
                    report = intersectY >= mbrY1 && intersectY < mbrY2;
                  } else {
                    report = false;
                  }
                }
                if (report) {
//                  Geometry lGeom = geomParser.parseGeom(lTuples[ii].get(lGeomColumn));
//                  Geometry rGeom = geomParser.parseGeom(rTuples[j].get(rGeomColumn));
//                  if (lGeom.overlaps(rGeom))
                    addToAnswer(output, lTuples[ii], rTuples[j]);
                }
              }
              ii++;
              progress();
            }
            j++;
          }
          progress();
        }
      }
    }
    return output;
  }

  
  private void addToAnswer(DataBag output, Tuple lTuple, Tuple rTuple) {
    List<Object> attrs = lTuple.getAll();
    attrs.addAll(rTuple.getAll());
    Tuple outTuple = tupleFactory.newTuple(attrs);
    output.add(outTuple);
  }


  private int detectGeomColumn(Tuple t) throws ExecException {
    for (int i = 0; i < t.size(); i++) {
      try {
        geomParser.parseGeom(t.get(i));
        return i;
      } catch (ExecException e) {
        // Nothing to do. Not in this column
      }
    }
    throw new ExecException("Cannot detect a geometry column in "+t);
  }
  
  public Schema outputSchema(Schema input) {
    try {
      // Column $0 is a bag of tuples that represent the left dataset
      Schema leftSchema = input.getField(0).schema.getField(0).schema;
      // Column $1 is a bag of tuples that represent the right dataset
      Schema rightSchema = input.getField(1).schema.getField(0).schema;
      
      Schema tupleSchema = new Schema();
      for (FieldSchema leftAttr : leftSchema.getFields()) {
        leftAttr = leftAttr.clone();
        leftAttr.alias = "left::"+leftAttr.alias;
        tupleSchema.add(leftAttr);
      }
      for (FieldSchema rightAttr : rightSchema.getFields()) {
        rightAttr = rightAttr.clone();
        rightAttr.alias = "right::"+rightAttr.alias;
        tupleSchema.add(rightAttr);
        
      }
      
      FieldSchema outSchema = new Schema.FieldSchema("result", tupleSchema);
      outSchema.type = DataType.BAG;
      
      return new Schema(outSchema);
    } catch (Exception e) {
      return null;
    }
  }

}
