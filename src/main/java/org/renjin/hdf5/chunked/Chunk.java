package org.renjin.hdf5.chunked;

import java.nio.DoubleBuffer;

/**
 * Chunk of data loaded into memory
 */
public class Chunk {


    private final ChunkKey key;
    private final DoubleBuffer values;

    public Chunk(ChunkKey key, DoubleBuffer values) {
        this.key = key;
        this.values = values;
    }

    public double getValues(int i) {
        return values.get(i);
    }

    public DoubleBuffer getValues() {
        return values;
    }

    public long[] getChunkOffset() {
        return key.getOffset();
    }

}
