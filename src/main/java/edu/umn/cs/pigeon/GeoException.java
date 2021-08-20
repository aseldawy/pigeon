package edu.umn.cs.pigeon;

import com.esri.core.geometry.ogc.OGCGeometry;
import org.locationtech.jts.geom.Geometry;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * An exception to signal an error with any geo function
 */
public class GeoException extends ExecException {
    public GeoException(Geometry geom, Exception e) {
        super("Error processing the shape " + geom == null? "<null>" : geom.toText(), e);
    }

    public GeoException(Geometry geom1, Geometry geom2, Exception e) {
        super("Error processing the shape " + (geom1 == null? "<null>" : geom1.toText())+
                " & "+(geom2 == null? "<null>" : geom2.toText()), e);
    }

    public GeoException(Geometry geom) {
        super("Error processing the shape " + (geom == null? "<null>" : geom.toText()));
    }

    public GeoException(OGCGeometry geom, Exception e) {
        super("Error processing the shape " + (geom == null? "<null>" : geom.asText()), e);
    }

    public GeoException(OGCGeometry geom1, OGCGeometry geom2, Exception e) {
        super("Error processing the shape " + (geom1 == null? "<null>" : geom1.asText())+
                " & "+(geom2 == null? "<null>" : geom2.asText()), e);
    }


    public GeoException(OGCGeometry geom) {
        super("Error processing the shape " + (geom == null? "<null>" : geom.asText()));
    }


    public GeoException(String message) {
        super(message);
    }

    public GeoException(Throwable e) {
        super(e);
    }
}
