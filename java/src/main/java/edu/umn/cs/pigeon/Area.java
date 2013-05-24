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
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * A UDF that returns the area of a geometry as calculated by
 * {@link Geometry#getArea()}
 * @author Ahmed Eldawy
 *
 */
public class Area extends EvalFunc<Double> {
  
  private final WKTReader wkt_reader = new WKTReader();

  @Override
  public Double exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      String wkt = null;
      if (v instanceof DataByteArray) {
        wkt = new String(((DataByteArray) v).get());
      } else if (v instanceof String) {
        wkt = (String) v;
      } else {
        int errCode = 2102;
        String msg = "Cannot parse a "+
        DataType.findTypeName(v) + " into a geometry";
        throw new ExecException(msg, errCode, PigException.BUG);
      }
      try {
        Geometry g = wkt_reader.read(wkt);
        return g.getArea();
      } catch (ParseException e) {
        throw new ExecException("Error parsing object from '" + wkt + "'");
      }
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
