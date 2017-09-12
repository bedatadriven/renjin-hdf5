package org.renjin.hdf5.message;

import org.renjin.hdf5.Flags;
import org.renjin.hdf5.HeaderReader;
import org.renjin.repackaged.guava.primitives.UnsignedBytes;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Created by alex on 11-9-17.
 */
public class DatatypeMessage extends Message {

    public static final int TYPE = 0x003;

    private final int version;
    private DataClass dataClass;
    private final int size;
    private ByteOrder byteOrder;
    private int signLocation;
    private int bitOffset;
    private int bitPrecision;
    private int exponentLocation;
    private int exponentSize;
    private int mantissaLocation;
    private int mantissaSize;
    private long exponentBias;

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
        Flags classBitField0 = reader.readFlags();
        Flags classBitField8 = reader.readFlags();
        Flags classBitField16 = reader.readFlags();
        size = reader.readUInt32AsInt();

        version = (classAndVersion & 0xF);
        int dataClassIndex = (classAndVersion >> 4) & 0xF;
        dataClass = DataClass.values()[dataClassIndex];

        if(dataClass == DataClass.FLOATING_POINT) {
            if(!classBitField0.isSet(0) && !classBitField0.isSet(6)) {
                byteOrder = ByteOrder.LITTLE_ENDIAN;
            } else if(classBitField0.isSet(0) && !classBitField0.isSet(6)) {
                byteOrder = ByteOrder.BIG_ENDIAN;
            } else {
                throw new UnsupportedOperationException("Unsupported endianness");
            }
            signLocation = UnsignedBytes.toInt(classBitField8.value());

            bitOffset = reader.readUInt16();
            bitPrecision = reader.readUInt16();
            exponentLocation = reader.readUInt8();
            exponentSize = reader.readUInt8();
            mantissaLocation = reader.readUInt8();
            mantissaSize = reader.readUInt8();
            exponentBias = reader.readUInt32();
        }
    }

    public DataClass getDataClass() {
        return dataClass;
    }

}
