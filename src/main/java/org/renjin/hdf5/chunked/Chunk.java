package org.renjin.hdf5.chunked;

/**
 * Chunk of data loaded into memory
 */
public abstract class Chunk {

    private final long[] chunkOffset;

    public Chunk(long[] chunkOffset) {
        this.chunkOffset = chunkOffset;
    }

    public long[] getChunkOffset() {
        return chunkOffset;
    }

    public abstract double getDoubleAt(int i);
}
