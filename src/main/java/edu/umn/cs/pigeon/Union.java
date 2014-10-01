/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

/**
 * Finds the union of a set of shapes. This is an algebraic function which works
 * more efficiently by computing sub-unions of some shapes and finally computing
 * the overall union.
 * @author Ahmed Eldawy
 *
 */
public class Union extends EvalFunc<DataByteArray> implements Algebraic,
    Accumulator<DataByteArray> {
  
  private static final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    return new DataByteArray(union(input).asBinary().array());
  }

  @Override
  public String getInitial() { return Initial.class.getName();}

  @Override
  public String getIntermed() { return Intermed.class.getName();}

  @Override
  public String getFinal() { return Final.class.getName(); }
  
  static public class Initial extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(Tuple input) throws IOException {
      // Retrieve the first element (tuple) in the given bag
      return ((DataBag)input.get(0)).iterator().next();
    }
  }

  static public class Intermed extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(Tuple input) throws IOException {
      return TupleFactory.getInstance().newTuple(
          new DataByteArray(union(input).asBinary().array()));
    }
  }
  
  static public class Final extends EvalFunc<DataByteArray> {
    @Override
    public DataByteArray exec(Tuple input) throws IOException {
      return new DataByteArray(union(input).asBinary().array());
    }
  }

  static protected OGCGeometry union(Tuple input) throws ExecException {
    DataBag values = (DataBag)input.get(0);
    if (values.size() == 0)
      return null;
    ArrayList<OGCGeometry> all_geoms = new ArrayList<OGCGeometry>();
    for (Tuple one_geom : values) {
      OGCGeometry parsedGeom = geometryParser.parseGeom(one_geom.get(0));
      all_geoms.add(parsedGeom);
    }
    
    // Do a union of all_geometries in the recommended way (using buffer(0))
    OGCGeometryCollection geom_collection = new OGCConcreteGeometryCollection(
        all_geoms, all_geoms.get(0).getEsriSpatialReference());
    return geom_collection.union(all_geoms.get(0));
  }

  OGCGeometry partialUnion;
  ESRIGeometryParser geomParser = new ESRIGeometryParser();
  
  @Override
  public void accumulate(Tuple b) throws IOException {
    // Union all passed elements along with the union we might currently have
    DataBag bag = (DataBag) b.get(0);
    ArrayList<OGCGeometry> all_geoms = new ArrayList<OGCGeometry>();
    if (partialUnion != null)
      all_geoms.add(partialUnion);
    for (Tuple t : bag) {
      OGCGeometry geom = geomParser.parseGeom(t.get(0));
      all_geoms.add(geom);
    }
    partialUnion = new OGCConcreteGeometryCollection(all_geoms, all_geoms
        .get(0).getEsriSpatialReference()).union(all_geoms.get(0));
  }

  @Override
  public DataByteArray getValue() {
    return new DataByteArray(partialUnion.asBinary().array());
  }

  @Override
  public void cleanup() {
    partialUnion = null;
  }
}
