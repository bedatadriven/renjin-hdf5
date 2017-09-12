package org.renjin.hdf5;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.commons.vfs2.provider.local.LocalFileSystem;
import org.renjin.eval.Context;
import org.renjin.eval.EvalException;
import org.renjin.hdf5.message.DatatypeMessage;
import org.renjin.hdf5.vector.ChunkedDataset;
import org.renjin.hdf5.vector.ChunkedDoubleVector;
import org.renjin.invoke.annotations.Current;
import org.renjin.sexp.Vector;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Entry point for Renjin-specific functionality.
 */
public class RenjinHdf5 {

    public static Vector readArray(@Current Context context, String file, String objectName) throws IOException {

        FileObject fileObject = context.resolveFile(file);
        if(!(fileObject instanceof LocalFile)) {
            throw new EvalException("Can only open HDF files from the local file system.");
        }
        LocalFile localFile = (LocalFile) fileObject;
        if(!localFile.exists()) {
            throw new EvalException("%s does not exist", localFile);
        }

        URL url = localFile.getURL();

        Hdf5File hdf5 = new Hdf5File(new File(url.getFile()));

        DataObject object = hdf5.getObject(objectName);
        DatatypeMessage datatype = object.getMessage(DatatypeMessage.class);
        if(!datatype.isDoubleIEE754()) {
            throw new EvalException("Unsupported data type. Currently only 64-bit floating point is implemented");
        }

        ChunkedDataset dataset = new ChunkedDataset(hdf5, object);
        return new ChunkedDoubleVector(dataset);
    }

}
