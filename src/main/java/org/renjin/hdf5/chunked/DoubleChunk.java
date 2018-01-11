package org.renjin.hdf5.chunked;

import java.nio.DoubleBuffer;

public class DoubleChunk extends Chunk {

  private final DoubleBuffer buffer;

  public DoubleChunk(long[] chunkOffset, DoubleBuffer buffer) {
    super(chunkOffset);
    this.buffer = buffer;
  }

  @Override
  public double getDoubleAt(int i) {
    return buffer.get(i);
  }
}
