package org.renjin.hdf5.chunked;

import java.io.IOException;

public abstract class ChunkIndex {

    public abstract Chunk chunkAt(long[] arrayIndex) throws IOException;
}
