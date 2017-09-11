package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

/**
 * Created by alex on 11-9-17.
 */
public class DatatypeMessage extends Message {

    public static final int TYPE = 0x003;

    private final int version;
    private DataClass dataClass;
    private final int size;

    public enum DataClass {
        FIXED_POINT,
        FLOATING_POINT,
        TIME,
        STRING,
        BIT_FIELD,
        OPAQUE,
        COMPOUND,
        REFERENCE,
        ENUMERATED,
        VARIABLE_LENGTH,
        ARRAY;
    }


    public DatatypeMessage(HeaderReader reader) throws IOException {
        byte classAndVersion = reader.readByte();
        byte classBitField0 = reader.readByte();
        byte classBitField8 = reader.readByte();
        byte classBitField16 = reader.readByte();
        size = reader.readUInt32AsInt();

        version = (classAndVersion & 0xF);
        int dataClassIndex = (classAndVersion << 4) & 0xF;
        dataClass = DataClass.values()[dataClassIndex];
    }

    public DataClass getDataClass() {
        return dataClass;
    }
}
