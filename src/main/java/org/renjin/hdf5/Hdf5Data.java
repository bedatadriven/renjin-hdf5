package org.renjin.hdf5;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Hdf5Data {
  private FileChannel channel;
  private Superblock superblock;

  public Hdf5Data(File file) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    channel = randomAccessFile.getChannel();
    superblock = new Superblock(channel);
  }

  public Superblock getSuperblock() {
    return superblock;
  }

  public DataObject objectAt(long address) throws IOException {
    return new DataObject(this, address);
  }

  public HeaderReader readerAt(long address) throws IOException {
    return readerAt(address, 1024 * 5);
  }

  public HeaderReader readerAt(long address, long maxHeaderSize) throws IOException {
    MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, address,
        Math.min(maxHeaderSize, channel.size() - address));

    return new HeaderReader(superblock, buffer);
  }

  public ByteBuffer bufferAt(long address, long size) throws IOException {
    return channel.map(FileChannel.MapMode.READ_ONLY, address, size);
  }

  public int read(ByteBuffer buffer, long address) throws IOException {
    return channel.read(buffer, address);
  }

  public MappedByteBuffer map(FileChannel.MapMode mode, long address, long size) throws IOException {
    return channel.map(mode, address, size);
  }
}
