/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.GeometryEngine;

/**
 * Converts an OGCGeometry to an ESRI Shape byte array so it can later be used
 * in Hive without the need to perform additional parsings in Hive.
 * @author Carlos Balduz
 */
public class OGCGeometryToESRIShapeParser {
  private static final int SIZE_WKID = 4;
  private static final int SIZE_TYPE = 1;

  public static byte[] ogcGeomToEsriShape(Object o) throws ExecException {
    ESRIGeometryParser parser = new ESRIGeometryParser();
    OGCGeometry geom = parser.parseGeom(o);
    int wkid = geom.SRID();
    int geomType = getGeometryType(geom);

    byte[] shape = GeometryEngine.geometryToEsriShape(geom.getEsriGeometry());
    byte[] shapeWithData = new byte[shape.length + SIZE_TYPE + SIZE_WKID];

    System.arraycopy(shape, 0, shapeWithData, SIZE_WKID + SIZE_TYPE, shape.length);    
    System.arraycopy(intToBytes(wkid), 0, shapeWithData, 0, SIZE_WKID);
    shapeWithData[SIZE_WKID] = (byte) type;

    return shapeWithData;
  }

  /**
   * Convert int to byte array.
   * @param i
   * @return
   */
  private static byte[] intToBytes(int value) {
    return new byte[] {
      (byte) (value >>> 24),
      (byte) (value >>> 16),
      (byte) (value >>> 8),
      (byte) value
    };
  }

  /**
   * Get the geometry type of a OGCGeometry object.
   * @param i
   * @return
   */
  private static int getGeometryType(OGCGeometry geom) {
    String typeName = geom.geometryType();
    int ogcType = 0;

    if (typeName.equals("Point"))
      ogcType = 1;
    else if (typeName.equals("LineString"))
      ogcType = 2;
    else if (typeName.equals("Polygon"))
      ogcType = 3;
    else if (typeName.equals("MultiPoint"))
      ogcType = 4;
    else if (typeName.equals("MultiLineString"))
      ogcType = 5;
    else if (typeName.equals("MultiPolygon"))
      ogcType = 6;

    return ogcType;
  }
}
