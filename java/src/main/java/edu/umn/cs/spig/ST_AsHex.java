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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * Returns the Well-Known Binary (WKB) representation of a geometry object
 * represented as hex string.
 * @author Ahmed Eldawy
 *
 */
public class ST_AsHex extends EvalFunc<String> {

  private GeometryParser geometryParser = new GeometryParser();
  private WKBWriter wkbWriter = new WKBWriter();
  
  @Override
  public String exec(Tuple t) throws IOException {
    try {
      if (t.size() != 1)
        throw new IOException("ST_AsText expects one geometry argument");
      Geometry geom = geometryParser.parseGeom(t.get(0));
      byte[] wkb = wkbWriter.write(geom);
      return WKBWriter.bytesToHex(wkb);
    } catch (ParseException e) {
      throw new IOException("Error parsing object "+t, e);
    }
  }

}
