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
 * A predicate that tells whether a specific geometry is empty or not.
 * A wrapper call to {@link OGCGeometry#isEmpty()}
 * @author Ahmed Eldawy
 *
 */
public class IsEmpty extends FilterFunc {
  
  private final GeometryParser geometryParser = new GeometryParser();

  @Override
  public Boolean exec(Tuple input) throws IOException {
    try {
      Object v = input.get(0);
      OGCGeometry geom = geometryParser.parseGeom(v);
      return geom.isEmpty();
    } catch (ExecException ee) {
      throw ee;
    }
  }

}
