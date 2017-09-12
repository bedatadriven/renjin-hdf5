package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;

public class DataStorageMessage extends Message {

    public static final int TYPE = 0x000B;

    public DataStorageMessage(HeaderReader reader) {
        byte version = reader.readByte();
        if(version != 1) {
            throw new UnsupportedOperationException("version: " + version);
        }
        int numFilters = reader.readUInt8();
        reader.readReserved(2);
        reader.readReserved(4);

        for (int i = 0; i < numFilters; i++) {
            int filterId = reader.readUInt16();
            int nameLength = reader.readUInt16();
            int flags = reader.readUInt16();
            int numClientDataValues = reader.readUInt16();

        }

    }

}
