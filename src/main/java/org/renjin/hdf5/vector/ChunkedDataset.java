package org.renjin.hdf5.vector;

import org.renjin.eval.EvalException;
import org.renjin.hdf5.DataObject;
import org.renjin.hdf5.Hdf5File;
import org.renjin.hdf5.chunked.Chunk;
import org.renjin.hdf5.chunked.ChunkIndex;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataspaceMessage;
import org.renjin.hdf5.message.DatatypeMessage;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.IntArrayVector;

import java.io.IOException;

/**
 * Manages access to a chunked HDF5 dataset.
 */
public class ChunkedDataset {

    private final DataspaceMessage dataspace;
    private final DatatypeMessage datatype;
    private final DataLayoutMessage layout;
    private final ChunkIndex chunkIndex;

    private final int nDim;
    private long dimensionSize[];
    private long chunkSize[];
    private long vectorLength;

    public ChunkedDataset(Hdf5File file, DataObject object) throws IOException {
        dataspace = object.getMessage(DataspaceMessage.class);
        datatype = object.getMessage(DatatypeMessage.class);
        layout = object.getMessage(DataLayoutMessage.class);

        /*
         * HDF5 lays out data in row-major order while R uses column-major order.
         * Following the convention of HDFArray, we will preserve the layout but transpose
         * the dimensions, so that will treat columns as rows and vice-versa.
         */
        nDim = dataspace.getDimensionality();
        vectorLength = 1;
        dimensionSize = new long[nDim];
        chunkSize = new long[nDim];
        for (int i = 0; i < nDim; i++) {
            dimensionSize[nDim - i - 1] = dataspace.getDimensionSize(i);
            chunkSize[nDim - i -1] = layout.getChunkSize(i);
            vectorLength *= dataspace.getDimensionSize(i);
        }

        chunkIndex = file.openChunkIndex(object);
    }

    private int checkedIntCast(long size) {
        if(size > Integer.MAX_VALUE) {
            throw new EvalException("Size too large: " + size);
        }
        return (int)size;
    }


    /**
     * Builds an R attribute list that includes this dataset's dimension.
     */
    public AttributeMap buildAttributes() {

        int[] intDims = new int[nDim];
        for (int i = 0; i < nDim; i++) {
            intDims[i] = checkedIntCast(dimensionSize[i]);
        }

        return new AttributeMap.Builder().setDim(new IntArrayVector(intDims)).build();
    }

    public int getVectorLength32() {
        return checkedIntCast(vectorLength);
    }

    public long[] vectorIndexToHdfsArrayIndex(long vectorIndex) {
        long arrayIndex[] = new long[nDim];
        for(int i = 0; i != nDim; ++i) {
            arrayIndex[nDim - i - 1] = vectorIndex % dimensionSize[i];
            vectorIndex = (vectorIndex - arrayIndex[nDim - i - 1]) / dimensionSize[i];
        }
        return arrayIndex;
    }

    public long hdfsArrayIndexToVectorIndex(long[] arrayIndex) {
        long vectorIndex = 0;
        long offset = 1;

        for(int i = 0; i != nDim; ++i) {
            vectorIndex += arrayIndex[nDim - i - 1] * offset;
            offset *= dimensionSize[i];
        }

        return vectorIndex;
    }

    public ChunkCursor chunkAt(int vectorIndex) throws IOException {
        long arrayIndex[] = vectorIndexToHdfsArrayIndex(vectorIndex);
        Chunk chunk = chunkIndex.chunkAt(arrayIndex);

        long vectorStart = hdfsArrayIndexToVectorIndex(chunk.getChunkOffset());
        long vectorLength = chunkSize[0];

        return new ChunkCursor(vectorStart, vectorLength, chunk);
    }
}
