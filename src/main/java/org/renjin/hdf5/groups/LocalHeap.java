package org.renjin.hdf5.groups;

import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

/**
 * A local heap is a collection of small pieces of data that are particular to a single object in the HDF5 file.
 * Objects can be inserted and removed from the heap at any time. The address of a heap does not change once the
 * heap is created. For example, a group stores addresses of objects in symbol table nodes with the names of
 * links stored in the group’s local heap.
 */
public class LocalHeap {

  private Hdf5Data file;
  private final long dataSegmentSize;
  private final long headOfFreeListOffset;
  private final long dataSegmentAddress;

  public LocalHeap(Hdf5Data file, long address) throws IOException {
    this.file = file;
    HeaderReader reader = file.readerAt(address, 48);
    reader.checkSignature("HEAP");
    int version = reader.readUInt8();
    if(version != 0) {
      throw new UnsupportedOperationException("Version: " + version);
    }
    reader.readReserved(3);
    dataSegmentSize = reader.readLength();
    headOfFreeListOffset = reader.readLength();

    dataSegmentAddress = reader.readOffset();

  }

  public String stringAt(long offset) throws IOException {
    HeaderReader reader = file.readerAt(dataSegmentAddress + offset);
    return reader.readNullTerminatedAsciiString();
  }
}
