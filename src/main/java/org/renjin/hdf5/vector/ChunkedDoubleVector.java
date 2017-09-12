package org.renjin.hdf5.vector;


import org.renjin.eval.EvalException;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.DoubleVector;
import org.renjin.sexp.SEXP;

import java.io.IOException;

public class ChunkedDoubleVector extends DoubleVector {

    private final ChunkedDataset dataset;
    private ChunkCursor chunk = null;

    public ChunkedDoubleVector(ChunkedDataset dataset) {
        super(dataset.buildAttributes());
        this.dataset = dataset;
    }

    public ChunkedDoubleVector(ChunkedDataset dataset, AttributeMap attributeMap) {
        super(attributeMap);
        this.dataset = dataset;
    }

    @Override
    protected SEXP cloneWithNewAttributes(AttributeMap attributeMap) {
        return new ChunkedDoubleVector(this.dataset, attributeMap);
    }

    @Override
    public double getElementAsDouble(int i) {

        if(chunk == null || !chunk.containsVectorIndex(i)) {
            try {
                chunk = dataset.chunkAt(i);
            } catch (IOException e) {
                throw new EvalException("I/O Error while accessing HDF5 File: " + e.getMessage(), e);
            }
        }

        return chunk.valueAt(i);
    }

    @Override
    public boolean isConstantAccessTime() {
        return true;
    }

    @Override
    public int length() {
        return dataset.getVectorLength32();
    }
}
