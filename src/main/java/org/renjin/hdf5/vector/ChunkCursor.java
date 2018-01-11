package org.renjin.hdf5.vector;


import org.renjin.hdf5.chunked.Chunk;

public class ChunkCursor {


    private final long vectorOffset;
    private final long vectorLength;
    private Chunk chunk;

    public ChunkCursor(long vectorOffset, long vectorLength, Chunk chunk) {

        this.vectorOffset = vectorOffset;
        this.vectorLength = vectorLength;
        this.chunk = chunk;
    }

    public boolean containsVectorIndex(int vectorIndex) {
        return vectorIndex >= vectorOffset && vectorIndex < (vectorOffset + vectorLength);
    }

    public double valueAt(int i) {
        return chunk.getDoubleAt(((int)(i - vectorOffset)));
    }
}
