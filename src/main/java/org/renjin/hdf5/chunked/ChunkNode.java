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

    public ChunkNode(DataLayoutMessage dataLayout, HeaderReader reader) throws IOException {
        reader.checkSignature("TREE");
        nodeType = reader.readByte();
        nodeLevel = reader.readUInt8();
        int entriesUsed = reader.readUInt16();
        addressLeftSibling = reader.readOffset();
        addressRightSibling = reader.readOffset();

        keys = new ChunkKey[entriesUsed + 1];

        for (int i = 0; i < entriesUsed + 1; i++) {
            keys[i] = new ChunkKey(reader,
                dataLayout.getDimensionality(),
                (i < entriesUsed));
        }
    }

    public boolean isLeaf() {
        return nodeLevel == 0;
    }

    public ChunkKey findChildAddress(long[] chunkCoordinates) {
        for (int i = 0; i < keys.length - 1; i++) {
            int lower = keys[i].compare(chunkCoordinates);
            int upper = keys[i+1].compare(chunkCoordinates);

            if(lower <= 0 && upper > 0) {
                return keys[i];
            }
        }
        throw new IllegalStateException();
    }
}
