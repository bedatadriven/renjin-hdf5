package org.renjin.hdf5.chunked;

import java.nio.IntBuffer;

public class Int32Chunk extends Chunk {

  private final IntBuffer buffer;
  private final long[] chunkOffset;

  public Int32Chunk(long[] chunkOffset, IntBuffer buffer) {
    super(chunkOffset);
    this.chunkOffset = chunkOffset;
    this.buffer = buffer;
  }

  @Override
  public double getDoubleAt(int i) {
    return buffer.get(i);
  }
}
