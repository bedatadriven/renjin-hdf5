package org.renjin.hdf5.chunked;

import java.nio.ByteBuffer;

public interface ChunkFactory {

  Chunk wrap(long[] chunkOffset, ByteBuffer buffer);
}
