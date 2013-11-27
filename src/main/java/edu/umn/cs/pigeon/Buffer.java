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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * A UDF that returns the minimal bounding rectangle (MBR) of a shape.
 * @author Ahmed Eldawy
 *
 */
public class Buffer extends EvalFunc<DataByteArray> {
  
  private final GeometryParser geometryParser = new GeometryParser();

  @Override
  public DataByteArray exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      OGCGeometry geom = geometryParser.parseGeom(v);
      double dist;
      Object distance = input.get(1);
      if (distance instanceof Double)
        dist = (Double) distance;
      else if (distance instanceof Float)
        dist = (Float) distance;
      else if (distance instanceof Integer)
        dist = (Integer) distance;
      else if (distance instanceof Long)
        dist = (Long) distance;
      else if (distance instanceof String)
        dist = Double.parseDouble((String) distance);
      else if (distance instanceof DataByteArray)
        dist = Double.parseDouble(new String(((DataByteArray) distance).get()));
      else
        throw new RuntimeException("Invalid second argument in call to Buffer. Expecting Double, Integer or Long");
      return new DataByteArray(geom.buffer(dist).asBinary().array());
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
