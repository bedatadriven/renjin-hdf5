package org.renjin.hdf5.groups;

import org.renjin.hdf5.DataObject;
import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.message.SymbolTableMessage;

import java.io.IOException;

public class GroupBTree implements GroupIndex {

  private final Hdf5Data file;
  private final SymbolTableMessage symbolTable;
  private final LocalHeap localHeap;
  private final Node rootNode;

  private class Node {

    private final int nodeLevel;
    private final long leftSibling;
    private final long rightSibling;
    private final SymbolTableNode[] keys;

    private Node(long address) throws IOException {
      HeaderReader reader = file.readerAt(address);
      reader.checkSignature("TREE");
      int nodeType = reader.readUInt8();
      if(nodeType != 0) {
        throw new IllegalStateException("Expected nodeType 0 (group nodes), found: " + nodeType);
      }

      nodeLevel = reader.readUInt8();
      int entriesUsed = reader.readUInt16();
      leftSibling = reader.readOffset();
      rightSibling = reader.readOffset();

      // For nodes of node type 0 (group nodes), the key is formatted as follows:
      // A single field of Size of Lengths bytes:
      //     Indicates the byte offset into the local heap for the first object name in the
      //     subtree which that key describes.

      keys = new SymbolTableNode[entriesUsed + 1];
      for (int i = 0; i < entriesUsed + 1; i++) {
        long nodeAddress = reader.readLength();
        if(nodeAddress != 0) {
          keys[i] = new SymbolTableNode(nodeAddress);
        }
      }
    }

    public boolean isLeaf() {
      return nodeLevel == 0;
    }

  }


  public class SymbolTableNode {

    private final SymbolTableEntry[] entries;

    private SymbolTableNode(long address) throws IOException {
      HeaderReader reader = file.readerAt(address);
      reader.checkSignature("SNOD");
      int version = reader.readUInt8();
      if(version != 1) {
        throw new UnsupportedOperationException("Version: " + version);
      }
      reader.readReserved(1);
      int numSymbols = reader.readUInt16();

      entries = new SymbolTableEntry[numSymbols];
      for (int i = 0; i < numSymbols; i++) {
        entries[i] = new SymbolTableEntry(reader);
      }
    }
  }


  public class SymbolTableEntry {

    private final long linkNameOffset;
    private final String linkName;
    private final long objectHeaderAddress;
    private final int cacheType;
    private final byte[] scratch;

    public SymbolTableEntry(HeaderReader reader) throws IOException {
      linkNameOffset = reader.readOffset();
      linkName = localHeap.stringAt(linkNameOffset);
      objectHeaderAddress = reader.readOffset();
      cacheType = reader.readUInt32AsInt();
      reader.readReserved(4);
      scratch = reader.readBytes(16);
    }
  }

  public GroupBTree(Hdf5Data file, SymbolTableMessage symbolTable) throws IOException {
    this.file = file;
    this.symbolTable = symbolTable;
    this.localHeap = new LocalHeap(file, symbolTable.getLocalHeapAddress());
    this.rootNode = new Node(symbolTable.getbTreeAddress());
  }


  @Override
  public DataObject getObject(String name) throws IOException {

    Node node = rootNode;
    while(!node.isLeaf()) {
      throw new UnsupportedOperationException("TODO");
    }

    SymbolTableNode symbolTableNode = findChildAddress(node, name);
    for (SymbolTableEntry entry : symbolTableNode.entries) {
      if(entry.linkName.equals(name)) {
        return file.objectAt(entry.objectHeaderAddress);
      }
    }


    throw new IllegalArgumentException(name);
  }

  public SymbolTableNode findChildAddress(Node node, String name) {

    if(node.keys.length == 2 && node.keys[0] == null) {
      return node.keys[1];
    }
    throw new UnsupportedOperationException("TODO");
//    for (int i = 0; i < node.keys.length - 1; i++) {
//      int lower = compare(node.keys[i], name);
//      int upper = compare(node.keys[i+1], name);
//
//      if(lower <= 0 && upper > 0) {
//        return node.keys[i];
//      }
//    }
//    throw new IllegalStateException();
  }

  private int compare(SymbolTableNode key, String name) {
    if(key == null) {
      return -1;
    }
    int first = key.entries[0].linkName.compareTo(name);
    if(first <= 0) {
      return first;
    }
    throw new UnsupportedOperationException("TODO");
  }
}
