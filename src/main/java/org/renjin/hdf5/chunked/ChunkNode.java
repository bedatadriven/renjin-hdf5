package org.renjin.hdf5.chunked;

import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.message.DataLayoutMessage;

import java.io.IOException;

public class ChunkNode {

    private final long addressLeftSibling;
    private final long addressRightSibling;
    private final int nodeLevel;
    private final byte nodeType;

    private ChunkKey keys[];
    private long childPointers[];

    public ChunkNode(DataLayoutMessage dataLayout, HeaderReader reader) throws IOException {
        reader.checkSignature("TREE");
        nodeType = reader.readByte();
        nodeLevel = reader.readUInt8();
        int entriesUsed = reader.readUInt16();
        addressLeftSibling = reader.readOffset();
        addressRightSibling = reader.readOffset();

        keys = new ChunkKey[entriesUsed + 1];
        childPointers = new long[entriesUsed];

        for (int i = 0; i < entriesUsed; i++) {
            keys[i] = new ChunkKey(reader, dataLayout.getDimensionality());
            childPointers[i] = reader.readOffset();
        }

        keys[entriesUsed] = new ChunkKey(reader, dataLayout.getDimensionality());
    }

}
