package org.renjin.hdf5.chunked;

import java.io.IOException;

public abstract class ChunkIndex {

    /**
     * Retrieves the chunk that includes the element at the given {@code arrayIndex}
     */
    public abstract Chunk chunkAt(long[] arrayIndex) throws IOException;
}
