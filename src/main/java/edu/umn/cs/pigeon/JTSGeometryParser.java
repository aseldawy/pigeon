/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.pigeon;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Retrieves a geometry from a pig attribute. It detects the type of the column
 * and the data stored in that column and automatically detects its format
 * and tries to get the geometry object from it. In particular, here are the
 * checks done in order:
 * 1- If the object is of type bytearray, it is parsed as a well known binary
 *    (WKB). If the parsing fails with a parse exception, the binary array
 *    is converted to a string and the next step is carried out.
 * 2- If the object is of type chararrray or step 1 fails, the string is parsed
 *    as a well known text (WKT). If the parsing fails and the string contains
 *    only hex characters (0-9 and A-Z), it is converted to binary and parsed
 *    as a well known binary (WKB).
 * @author Ahmed Eldawy
 *
 */
public class JTSGeometryParser {
  
  private final WKTReader wkt_reader = new WKTReader();
  private final WKBReader wkb_reader = new WKBReader();
  
  public Geometry parseGeom(Object o) throws ExecException {
    if (o instanceof DataByteArray) {
      byte[] bytes = ((DataByteArray) o).get();
      try {
        // Parse data as well known binary (WKB)
        return wkb_reader.read(bytes);
      } catch (ParseException e) {
        // Convert bytes to text and try text parser
        o = new String(bytes);
      }
    }
    if (o instanceof String) {
      try {
        // Parse string as well known text (WKT)
        return wkt_reader.read((String) o);
      } catch (ParseException e) {
        // Parse string as a hex string of a well known binary (WKB)
        String hex = (String) o;
        boolean isHex = true;
        for (int i = 0; isHex && i < hex.length(); i++) {
          char digit = hex.charAt(i);
          isHex = (digit >= '0' && digit <= '9') ||
            (digit >= 'a' && digit <= 'f') ||
            (digit >= 'A' && digit <= 'F');
        }
        if (isHex) {
          byte[] binary = WKBReader.hexToBytes(hex);
          try {
            return wkb_reader.read(binary);
          } catch (ParseException e1) {
            throw new ExecException("Error parsing '"+o+"'", e);
          }
        }
      }
    }
    throw new ExecException("Error parsing unknown type '"+o+"'");
  }
  
  public static double parseDouble(Object o) {
    if (o instanceof Integer)
      return (Integer)o;
    if (o instanceof Double)
      return (Double)o;
    if (o instanceof DataByteArray)
      return Double.parseDouble(new String(((DataByteArray) o).get()));
    throw new RuntimeException("Cannot parse "+o+" into double");
  }
}
