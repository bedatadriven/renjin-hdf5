package org.renjin.hdf5.chunked;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;


public class ChunkKey {
    private int chunkSize;
    private int filterMask;
    private long index[];

    public ChunkKey(HeaderReader reader, int dimensionality) throws IOException {
        chunkSize = reader.readUInt32AsInt();
        filterMask = reader.readUInt32AsInt();
        index = new long[dimensionality];
        for (int j = 0; j < dimensionality; j++) {
            index[j] = reader.readUInt64();
        }
        long zero = reader.readUInt64();
    }
}
