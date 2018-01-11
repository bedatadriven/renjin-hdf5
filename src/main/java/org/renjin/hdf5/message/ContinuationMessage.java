package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

/**
 * The object header continuation is the location in the file of a block containing more header messages for the
 * current data object. This can be used when header blocks become too large or are likely to change over time.
 */
public class ContinuationMessage extends Message  {

  public static final int MESSAGE_TYPE = 0x10;
  private final long offset;
  private final long length;


  public ContinuationMessage(HeaderReader reader) throws IOException {
    offset = reader.readOffset();
    length = reader.readLength();
  }

  public long getLength() {
    return length;
  }

  public long getOffset() {
    return offset;
  }
}
