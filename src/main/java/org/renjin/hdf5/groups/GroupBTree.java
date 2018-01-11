package org.renjin.hdf5.groups;

import org.renjin.hdf5.DataObject;
import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.message.SymbolTableMessage;

import java.io.IOException;

public class GroupBTree implements GroupIndex {

  private Hdf5Data file;
  private SymbolTableMessage symbolTable;
  private final Node rootNode;


  private class Node {

    private Node(long address) throws IOException {
      HeaderReader reader = file.readerAt(address);
      reader.checkSignature("TREE");
      int nodeType = reader.readUInt8();
      int nodeLevel = reader.readUInt8();
      int entriesUsed = reader.readUInt16();
      long left = reader.readOffset();
      long right = reader.readOffset();

      long keys[] = new long[entriesUsed + 1];
      for (int i = 0; i < entriesUsed + 1; i++) {
        keys[i] = reader.readOffset();
      }
    }
  }

  public GroupBTree(Hdf5Data file, SymbolTableMessage symbolTable) throws IOException {
    this.file = file;
    this.symbolTable = symbolTable;
    this.rootNode = new Node(symbolTable.getbTreeAddress());
  }


  @Override
  public DataObject getObject(String name) {
    throw new UnsupportedOperationException("TODO");
  }
}
