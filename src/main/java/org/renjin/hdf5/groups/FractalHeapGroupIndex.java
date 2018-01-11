package org.renjin.hdf5.groups;

import org.renjin.hdf5.DataObject;
import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.message.LinkMessage;

import java.io.IOException;

public class FractalHeapGroupIndex implements GroupIndex {

  private final FractalHeap heap;
  private Hdf5Data file;

  public FractalHeapGroupIndex(Hdf5Data file, long address) throws IOException {
    this.file = file;
    heap = new FractalHeap(file, address);

  }


  @Override
  public DataObject getObject(String name) throws IOException {
    FractalHeap.DirectBlock rootBlock = heap.getRootBlock();
    LinkMessage link = rootBlock.readLinkMessage();
    if(link.getLinkName().equals(name)) {
      return file.objectAt(link.getAddress());
    }
    throw new IllegalArgumentException(name);
  }
}
