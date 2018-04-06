/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

/**
 * Finds the convex null of a set of shapes. This is an algebraic function which
 * works more efficiently by computing convex hull of a subset of the shapes and
 * finally computing the overall convex hull.
 * 
 * @author Ahmed Eldawy
 * 
 */
public class ConvexHull extends EvalFunc<DataByteArray> implements Algebraic {
  
  private static final ESRIGeometryParser geometryParser = new ESRIGeometryParser();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    OGCGeometry geom = null;
    try {
      if (input.get(0) instanceof DataBag) {
        return new DataByteArray(convexHull(input).asBinary().array());
      }
      geom = geometryParser.parseGeom(input.get(0));
      try {
        return new DataByteArray(geom.convexHull().asBinary().array());
      } catch (ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        throw new RuntimeException(geom.asText(), e);
      }
    } catch (GeometryException e) {
      throw new GeoException(geom, e);
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new GeoException(geom, e);
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
      return TupleFactory.getInstance().newTuple(
          new DataByteArray(convexHull(input).asBinary().array()));
    }
  }
  
  static public class Final extends EvalFunc<DataByteArray> {
    @Override
    public DataByteArray exec(Tuple input) throws IOException {
      return new DataByteArray(convexHull(input).asBinary().array());
    }
  }

  static protected OGCGeometry convexHull(Tuple input) throws ExecException {
    DataBag values = (DataBag)input.get(0);
    if (values.size() == 0)
      return null;
    ArrayList<OGCGeometry> all_geoms =
        new ArrayList<OGCGeometry>();
    for (Tuple one_geom : values) {
      OGCGeometry parsedGeom = geometryParser.parseGeom(one_geom.get(0));
      all_geoms.add(parsedGeom);
    }
    
    // Do a convex null of all_geometries
    OGCGeometryCollection geom_collection = new OGCConcreteGeometryCollection(
        all_geoms, all_geoms.get(0).getEsriSpatialReference());
    return geom_collection.convexHull();
  }

}
