package org.renjin.hdf5.chunked;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;


public class ChunkKey {
    private int chunkSize;
    private int filterMask;
    private long index[];
    private long address;

    public ChunkKey(HeaderReader reader, int dimensionality, boolean hasAddress) throws IOException {
        chunkSize = reader.readUInt32AsInt();
        filterMask = reader.readUInt32AsInt();
        index = new long[dimensionality];
        for (int j = 0; j < dimensionality; j++) {
            index[j] = reader.readUInt64();
        }
        long zero = reader.readUInt64();
        if(hasAddress) {
            this.address = reader.readOffset();
        }
    }

    public long getAddress() {
        return address;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int compare(long[] chunkCoordinates) {
        for (int i = 0; i < index.length; i++) {
            if(index[i] != chunkCoordinates[i]) {
                return Long.compare(index[i], chunkCoordinates[i]);
            }
        }
        return 0;
    }


    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("ChunkKey{");
        for (int i = 0; i < index.length; i++) {
            if (i > 0) {
                s.append(",");
            }
            s.append(index[i]);
        }
        s.append("}");
        return s.toString();
    }
}
