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

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;


/**
 * Returns the size of a geometry.
 * For {@link OGCPoint} it always returns one.
 * For {@link OGCLineString} it returns number of points.
 * For {@link OGCPolygon} it returns number of edges.
 * @author Ahmed Eldawy
 *
 */
public class Size extends EvalFunc<Integer> {
  
  private final GeometryParser geometryParser = new GeometryParser();

  @Override
  public Integer exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      OGCGeometry geom = geometryParser.parseGeom(v);
      if (geom instanceof OGCPoint) {
        return 1;
      } else if (geom instanceof OGCLineString) {
        return ((OGCLineString)geom).numPoints();
      } else {
        throw new ExecException("size() not defined for shapes of type: "+geom.getClass());
      }
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
