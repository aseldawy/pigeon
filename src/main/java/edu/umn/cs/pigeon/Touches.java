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

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCGeometry;


/**
 * A UDF that tests whether a geometry touches another geometry or not
 * @author Tuan Pham
 *
 */
public class Touches extends FilterFunc {
  
  private final GeometryParser geometryParser = new GeometryParser();

  @Override
  public Boolean exec(Tuple input) throws IOException {
    try {
      OGCGeometry geom1 = geometryParser.parseGeom(input.get(0));
      OGCGeometry geom2 = geometryParser.parseGeom(input.get(1));
      return geom1.touches(geom2);
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
