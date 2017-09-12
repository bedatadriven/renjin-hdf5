package org.renjin.hdf5.chunked;

/**
 * Chunk of data loaded into memory
 */
public class Chunk {


    private final long[] chunkOffset;
    private final double[] values;

    public Chunk(long[] chunkOffset, double[] values) {
        this.chunkOffset = chunkOffset;
        this.values = values;
    }

    public double getValues(int i) {
        return values[i];
    }

    public double[] getValues() {
        return values;
    }

    public long[] getChunkOffset() {
        return chunkOffset;
    }

}
