package org.renjin.hdf5.message;


import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

public class LinkInfoMessage extends Message {

    public static final int MESSAGE_TYPE = 0x02;

    private static final int FLAG_CREATION_ORDER_TRACKED = 0x1;
    private static final int FLAG_CREATION_ORDER_INDEXED = 0x2;
    private final byte version;
    private final byte flags;
    private long maximumCreationIndex;
    private final long fractalHeapAddress;
    private final long nameIndexAddress;
    private long creationOrderIndex;

    public LinkInfoMessage(HeaderReader reader) throws IOException {
        version = reader.readByte();
        if(version != 0) {
            throw new UnsupportedOperationException("Version: " + version);
        }
        flags = reader.readByte();

        maximumCreationIndex = -1;
        if( (flags & FLAG_CREATION_ORDER_TRACKED) != 0) {
            maximumCreationIndex = reader.readUInt64();
        }

        fractalHeapAddress = reader.readOffset();
        nameIndexAddress = reader.readOffset();

        creationOrderIndex = -1;
        if( (flags & FLAG_CREATION_ORDER_INDEXED) != 0) {
            creationOrderIndex = reader.readOffset();
        }
    }

}
