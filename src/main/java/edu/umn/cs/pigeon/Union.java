/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
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
  
  private static final GeometryParser geometryParser = new GeometryParser();

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
  GeometryParser geomParser = new GeometryParser();
  
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
