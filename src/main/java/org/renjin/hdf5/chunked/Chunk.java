package org.renjin.hdf5.chunked;

/**
 * Chunk of data loaded into memory
 */
public class Chunk {


    private final ChunkKey key;
    private final double[] values;

    public Chunk(ChunkKey key, double[] values) {
        this.key = key;
        this.values = values;
    }

    public double getValues(int i) {
        return values[i];
    }

    public double[] getValues() {
        return values;
    }

    public long[] getChunkOffset() {
        return key.getOffset();
    }

}
