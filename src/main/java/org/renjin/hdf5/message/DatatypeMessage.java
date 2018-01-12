package org.renjin.hdf5.message;

import org.renjin.hdf5.Flags;
import org.renjin.hdf5.HeaderReader;
import org.renjin.repackaged.guava.primitives.UnsignedBytes;

import java.io.IOException;
import java.nio.ByteOrder;


public class DatatypeMessage extends Message {

    public static final int MESSAGE_TYPE = 0x003;

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

    private boolean signed;



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


        /*
         * The version of the datatype message and the datatype’s class information are packed together in this field.
         * The version number is packed in the top 4 bits of the field and the class is contained in the bottom 4 bits.
         */
        byte classAndVersion = reader.readByte();
        int version = (classAndVersion >> 4) & 0xF;
        int dataClassIndex = (classAndVersion & 0xF);

        if(version != 1) {
            throw new UnsupportedOperationException("data type message version: " + version);
        }

        Flags classBitField0 = reader.readFlags();
        Flags classBitField8 = reader.readFlags();
        Flags classBitField16 = reader.readFlags();
        size = reader.readUInt32AsInt();

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

        } else if(dataClass == DataClass.FIXED_POINT) {
            if(classBitField0.isSet(0)) {
                byteOrder = ByteOrder.BIG_ENDIAN;
            } else {
                byteOrder = ByteOrder.LITTLE_ENDIAN;
            }
            signed = classBitField0.isSet(3);
        }
    }

    public DataClass getDataClass() {
        return dataClass;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getSize() {
        return size;
    }


    public boolean isDoubleIEE754() {
        return dataClass == DataClass.FLOATING_POINT &&
               size == 8;
    }

    public boolean isSignedInteger32() {
        return dataClass == DataClass.FIXED_POINT &&
            size == 4 &&
            signed;
    }

    public ByteOrder getByteOrder() {
      return byteOrder;
    }


  @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Datatype{");
        if(isDoubleIEE754()) {
            sb.append("IEE754 Double");
        } else {
            if(!signed) {
                sb.append("UNSIGNED ");
            }
            sb.append(dataClass);
            sb.append("(");
            sb.append(size);
            sb.append(")");
        }
        sb.append("}");
        return sb.toString();
    }
}
