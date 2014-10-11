/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/

package edu.umn.cs.pigeon;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Vector;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;
import org.junit.Test;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;

public class TestHelper extends TestCase {
  @Test
  public void testTest() {
    assertTrue(true);
  }
  
  public static void assertGeometryEqual(Object expected, Object test) {
    OGCGeometry expected_geom = expected instanceof String?
        OGCGeometry.fromText((String)expected) :
        OGCGeometry.fromBinary(ByteBuffer.wrap(((DataByteArray)expected).get()));
    OGCGeometry test_geom = test instanceof String?
        OGCGeometry.fromText((String)test) :
        OGCGeometry.fromBinary(ByteBuffer.wrap(((DataByteArray)test).get()));
    if (expected_geom instanceof OGCGeometryCollection &&
        test_geom instanceof OGCGeometryCollection) {
      OGCGeometryCollection expected_coln = (OGCGeometryCollection) expected_geom;
      OGCGeometryCollection test_coln = (OGCGeometryCollection) test_geom;
      assertEquals(expected_coln.numGeometries(), test_coln.numGeometries());
      Vector<OGCGeometry> expectedGeometries = new Vector<OGCGeometry>();
      for (int i = 0; i < expected_coln.numGeometries(); i++) {
        expectedGeometries.add(expected_coln.geometryN(i));
      }
      for (int i = 0; i < test_coln.numGeometries(); i++) {
        OGCGeometry geom = test_coln.geometryN(i);
        int j = 0;
        while (j < expectedGeometries.size() && !geom.equals(expectedGeometries.get(j)))
          j++;

        assertTrue(j < expectedGeometries.size());
        expectedGeometries.remove(j++);
      }
    } else {
      assertTrue("Exepcted geometry to be '"+expected+"' but found '"+test+"'",
          expected_geom.equals(test_geom));
    }
  }

  private static String join(String delimiter, String[] strings) {
    String string = strings[0].toString();
    for (int i = 1; i < strings.length; i++) {
      string += delimiter + strings[i].toString();
    }
    return string;
  }

  public static String createTempFile(ArrayList<String[]> myData,
      String delimiter) throws Exception {
    File tmpFile = File.createTempFile("test", ".txt");
    if (tmpFile.exists()) {
      tmpFile.delete();
    }
    PrintWriter pw = new PrintWriter(tmpFile);
    for (int i = 0; i < myData.size(); i++) {
      pw.println(join(delimiter, myData.get(i)));
      System.err.println(join(delimiter, myData.get(i)));
    }
    pw.close();
    tmpFile.deleteOnExit();
    return tmpFile.getAbsolutePath();
  }
}
