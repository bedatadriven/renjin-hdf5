package org.renjin.hdf5.vector;


import java.nio.DoubleBuffer;

public class ChunkCursor {


    private final long vectorOffset;
    private final long vectorLength;
    private final DoubleBuffer values;

    public ChunkCursor(long vectorOffset, long vectorLength, DoubleBuffer values) {

        this.vectorOffset = vectorOffset;
        this.vectorLength = vectorLength;
        this.values = values;
    }

    public boolean containsVectorIndex(int vectorIndex) {
        return vectorIndex >= vectorOffset && vectorIndex < (vectorOffset + vectorLength);
    }

    public double valueAt(int i) {
        return values.get((int)(i - vectorOffset));
    }
}
