package org.renjin.hdf5;

import org.renjin.hdf5.vector.ChunkedDataset;
import org.renjin.hdf5.vector.ChunkedDoubleVector;
import org.renjin.sexp.Vector;

import java.io.File;
import java.io.IOException;

/**
 * Entry point for Renjin-specific functionality.
 */
public class RenjinHdf5 {

    public static Vector readArray(String file, String objectName) throws IOException {

        Hdf5File hdf5 = new Hdf5File(new File(file));

        DataObject object = hdf5.getObject(objectName);
        ChunkedDataset dataset = new ChunkedDataset(hdf5, object);
        return new ChunkedDoubleVector(dataset);
    }
}
