package org.renjin.hdf5.vector;


public class ChunkCursor {


    private final long vectorOffset;
    private final long vectorLength;
    private final double[] values;

    public ChunkCursor(long vectorOffset, long vectorLength, double[] values) {

        this.vectorOffset = vectorOffset;
        this.vectorLength = vectorLength;
        this.values = values;
    }

    public boolean containsVectorIndex(int vectorIndex) {
        return vectorIndex >= vectorOffset && vectorIndex < (vectorOffset + vectorLength);
    }

    public double valueAt(int i) {
        return values[((int)(i - vectorOffset))];
    }
}
