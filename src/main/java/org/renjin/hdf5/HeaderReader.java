package org.renjin.hdf5;



import org.renjin.repackaged.guava.base.Charsets;
import org.renjin.repackaged.guava.primitives.UnsignedBytes;
import org.renjin.repackaged.guava.primitives.UnsignedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class HeaderReader {

    private Superblock superblock;
    private ByteBuffer buffer;

    public HeaderReader(Superblock superblock, ByteBuffer buffer) {
        this.superblock = superblock;
        this.buffer = buffer;
        this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public byte readByte() {
        return buffer.get();
    }

    public void checkSignature(String signature) throws IOException {
        byte[] expected = signature.getBytes(Charsets.US_ASCII);
        byte array[] = readBytes(expected.length);

        for (int i = 0; i < expected.length; i++) {
            if(expected[i] != array[i]) {
                throw new IOException("Invalid signature. Expected: " + signature);
            }
        }
    }

    public Flags readFlags() {
        return new Flags(buffer.get());
    }

    public int readInt() {
        return buffer.getInt();
    }

    public int readUInt16() {
        return buffer.getChar();
    }

    public int readUInt8() {
        return UnsignedBytes.toInt(buffer.get());
    }

    public long readUInt32() {
        return UnsignedInts.toLong(buffer.getInt());
    }

    public long readUInt64() throws IOException {
        long value = buffer.getLong();
        if(value < 0) {
            throw new IOException("Unsigned long overflow");
        }
        return value;
    }

    public int readVariableLengthSizeAsInt(Flags flags) throws IOException {
        long length = readVariableLengthSize(flags.value() & 0x3);
        if(length > Integer.MAX_VALUE) {
            throw new IOException("Overflow");
        }
        return (int)length;
    }

    public long readVariableLengthSize(int size) throws IOException {
        switch (size) {
            case 0:
                return readUInt8();
            case 1:
                return readUInt16();
            case 2:
                return readUInt32();
            case 3:
                return readUInt64();

            default:
                throw new IllegalArgumentException("size: " + size);
        }
    }

    public void updateLimit(int length) {
        buffer.limit(buffer.position() + length);
    }

    public int remaining() {
        return buffer.remaining();
    }

    public byte[] readBytes(int size) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return bytes;
    }

    public long readOffset() throws IOException {
        if(superblock.getOffsetSize() == 4) {
            return readUInt32();
        }
        long value = buffer.getLong();
        // -1 is the UNSPECIFIED value
        if(value < -1) {
            throw new IOException("Long offset overflow");
        }
        return value;
    }

    public long readLength() throws IOException {
        if(superblock.getLengthSize() == 4) {
            return readUInt32();
        }
        return readUInt64();
    }

    public String readString(int length, Charset charset) {
        byte[] bytes = readBytes(length);
        return new String(bytes, charset);
    }

    public byte peekByte() {
        return buffer.get(0);
    }

    public void readReserved(int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            byte reserved = readByte();
        }
    }

    public int position() {
        return buffer.position();
    }

    public int readUInt() {
        return buffer.getInt();
    }

    public int[] readIntArray(int count) {
        int array[] = new int[count];
        for (int i = 0; i < count; i++) {
            array[i] = readUInt();
        }
        return array;
    }

    public int readUInt32AsInt() throws IOException {
        long value = readUInt32();
        if(value > Integer.MAX_VALUE) {
            throw new IOException("Integer overflow");
        }
        return (int)value;
    }

    public String readNullTerminatedAsciiString(int nameLength) {
        byte bytes[] = readBytes(nameLength);
        int len = 0;
        while(bytes[len] == 0 && len < bytes.length) {
            len++;
        }
        return new String(bytes, 0, len, Charsets.US_ASCII);
    }

    /**
     * Reads an integer of the given size
     * @param byteSize the size of the integer in bytes
     */
    public long readUInt(int byteSize) throws IOException {
        switch (byteSize) {
            case 1:
                return readUInt8();
            case 2:
                return readUInt16();
            case 4:
                return readUInt32();
            case 8:
                return readUInt64();
            default:
                throw new IllegalArgumentException("bytes: " + byteSize);
        }
    }
}
