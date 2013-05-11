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
package edu.umn.cs.spig;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * Finds the union of a set of shapes. This is an algebraic function which works
 * more efficiently by computing sub-unions of some shapes and finally computing
 * the overall union.
 * @author Ahmed Eldawy
 *
 */
public class ST_Union extends EvalFunc<DataByteArray> implements Algebraic,
    Accumulator<DataByteArray> {
  
  private static final GeometryParser geometryParser = new GeometryParser();
  private static final WKBWriter wkbWriter = new WKBWriter();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    try {
      return new DataByteArray(wkbWriter.write(union(input)));
    } catch (ParseException e) {
      throw new IOException("Error computing union", e);
    }
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
      try {
        return TupleFactory.getInstance().newTuple(union(input).toString());
      } catch (ParseException e) {
        throw new IOException("Error computing union", e);
      }
    }
  }
  
  static public class Final extends EvalFunc<DataByteArray> {
    @Override
    public DataByteArray exec(Tuple input) throws IOException {
      try {
        return new DataByteArray(wkbWriter.write(union(input)));
      } catch (ParseException e) {
        throw new IOException("Error computing union", e);
      }
    }
  }

  static protected Geometry union(Tuple input) throws ExecException, ParseException {
    DataBag values = (DataBag)input.get(0);
    if (values.size() == 0)
      return null;
    Geometry[] all_geoms = new Geometry[(int) values.size()];
    int i = 0;
    for (Tuple one_geom : values) {
      all_geoms[i++] = geometryParser.parseGeom(one_geom.get(0));
    }
    
    // Do a union of all_geometries in the recommended way (using buffer(0))
    GeometryCollection geom_collection =
        new GeometryFactory().createGeometryCollection(all_geoms);
    return geom_collection.buffer(0);
  }

  Geometry partialUnion;
  GeometryParser geomParser = new GeometryParser();
  GeometryFactory geometryFactory = new GeometryFactory();
  
  @Override
  public void accumulate(Tuple b) throws IOException {
    try {
      // Union all passed elements along with the union we might currently have
      DataBag bag = (DataBag) b.get(0);
      Geometry[] all_geoms = new Geometry[(int) bag.size()
          + (partialUnion == null ? 0 : 1)];
      int i = 0;
      if (partialUnion != null)
        all_geoms[i++] = partialUnion;
      for (Tuple t : bag) {
        Geometry geom = geomParser.parseGeom(t.get(0));
        all_geoms[i++] = geom;
      }
      partialUnion = geometryFactory.createGeometryCollection(all_geoms).buffer(0);
    } catch (ParseException e) {
      throw new IOException("Error parsing object: "+b, e);
    }
  }

  @Override
  public DataByteArray getValue() {
    return new DataByteArray(wkbWriter.write(partialUnion));
  }

  @Override
  public void cleanup() {
    partialUnion = null;
  }
}
