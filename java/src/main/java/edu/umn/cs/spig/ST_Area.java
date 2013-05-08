/**
 * 
 */
package edu.umn.cs.spig;

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
 * @author eldawy
 *
 */
public class ST_Area extends EvalFunc<Double> {
  
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
