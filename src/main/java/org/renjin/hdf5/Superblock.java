package org.renjin.hdf5;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class Superblock {

    private static final long MAX_LENGTH = 1000;

    private final byte offsetsSize;
    private final byte lengthsSize;
    private final byte fileConsistencyFlags;
    private final long baseAddress;
    private final long superBlockExtensionAddress;
    private final long endOfFileAddress;
    private final long rootGroupObjectHeaderAddress;
    private final int superBlockChecksum;

    public Superblock(FileChannel channel) throws IOException {

        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, MAX_LENGTH);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        readAndCheckSignature(buffer);

        byte superBlockVersion = buffer.get();

        if(superBlockVersion != 2) {
            throw new IOException("Unsupported superblock version: " + superBlockVersion);
        }

        /*
         * This value contains the number of bytes used to store addresses in the file. The values for the
         * addresses of objects in the file are offsets relative to a base address, usually the address of the
         * superblock signature. This allows a wrapper to be added after the file is created without invalidating
         * the internal offset locations.
         */
        offsetsSize = buffer.get();
        lengthsSize = buffer.get();
        fileConsistencyFlags = buffer.get();

        if(offsetsSize != 8 || lengthsSize != 8) {
            throw new IOException("Unsupported offsets/length size: " + offsetsSize);
        }

        baseAddress = buffer.getLong();
        superBlockExtensionAddress = buffer.getLong();
        endOfFileAddress = buffer.getLong();
        rootGroupObjectHeaderAddress = buffer.getLong();
        superBlockChecksum = buffer.getInt();
    }

    private void readAndCheckSignature(ByteBuffer buffer) throws IOException {
        byte[] array = new byte[8];
        buffer.get(array);

        if(! ( /*array[0] == 137 && */
            array[1] == 'H' &&
                array[2] == 'D' &&
                array[3] == 'F' &&
                array[4] == '\r' &&
                array[5] == '\n' &&
                array[6] == 0x1a &&
                array[7] == '\n')) {
            throw new IOException("Invalid format signature: " + Arrays.toString(array));
        }
    }

    public long getBaseAddress() {
        return baseAddress;
    }

    public byte getOffsetSize() {
        return offsetsSize;
    }

    public byte getLengthSize() {
        return lengthsSize;
    }

    public long getRootGroupObjectHeaderAddress() {
        return rootGroupObjectHeaderAddress;
    }
}
