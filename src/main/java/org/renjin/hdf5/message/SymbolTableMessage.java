package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

/**
 * Each “old style” group has a v1 B-tree and a local heap for storing symbol table entries,
 * which are located with this message.
 */
public class SymbolTableMessage extends Message {
  public static final int MESSAGE_TYPE = 0x11;
  private final long bTreeAddress;
  private final long localHeapAddress;

  public SymbolTableMessage(HeaderReader reader) throws IOException {
    bTreeAddress = reader.readOffset();
    localHeapAddress = reader.readOffset();
  }

  public long getbTreeAddress() {
    return bTreeAddress;
  }

  public long getLocalHeapAddress() {
    return localHeapAddress;
  }
}
