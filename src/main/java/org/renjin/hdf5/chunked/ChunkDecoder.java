package org.renjin.hdf5.chunked;

import java.io.IOException;
import java.util.zip.DataFormatException;

/**
 * Responsible for decoding chunks, decompressing, etc as necessary
 */
public interface ChunkDecoder {

  Chunk read(long[] chunkOffset, long address, int size) throws IOException;

}
