package org.renjin.hdf5.chunked;

import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.Hdf5File;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.DataFormatException;

public class UncompressedDecoder implements ChunkDecoder {

  private final Hdf5Data file;
  private final ChunkFactory factory;

  public UncompressedDecoder(Hdf5Data file, ChunkFactory factory) {
    this.file = file;
    this.factory = factory;
  }

  @Override
  public Chunk read(long[] chunkOffset, long address, int size) throws IOException {
    MappedByteBuffer buffer = file.map(FileChannel.MapMode.READ_ONLY, address, size);
    return factory.wrap(chunkOffset, buffer);
  }
}
