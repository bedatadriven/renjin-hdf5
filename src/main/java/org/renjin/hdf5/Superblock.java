package org.renjin.hdf5;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class Superblock {

    private static final long MAX_LENGTH = 1000;

    private final byte superBlockVersion;
    private byte offsetsSize;
    private byte lengthsSize;
    private byte fileConsistencyFlags;
    private long baseAddress;
    private long superBlockExtensionAddress;
    private long endOfFileAddress;
    private long rootGroupObjectHeaderAddress;
    private int superBlockChecksum;
    private long driverInformationBlockAddress;

    public Superblock(FileChannel channel) throws IOException {

        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Math.min(channel.size(), MAX_LENGTH));
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        readAndCheckSignature(buffer);

        superBlockVersion = buffer.get();

        if(superBlockVersion == 0) {
            readVersion0(buffer);
        } else if(superBlockVersion == 2 || superBlockVersion == 3) {
            readVersion2(buffer);
        } else {
            throw new IOException("Unsupported superblock version: " + superBlockVersion);
        }
    }

    private void readVersion0(MappedByteBuffer buffer) throws IOException {

        int freeSpaceStorageVersion = buffer.get();
        int rootGroupSymbolTableEntryVersion = buffer.get();
        byte reserved = buffer.get();

        int sharedHeaderMessageFormatVersion = buffer.get();
        offsetsSize = buffer.get();
        lengthsSize = buffer.get();
        byte reserved1 = buffer.get();

        int groupLeafNodeK = buffer.getShort();
        int groupInternalNodeK = buffer.getShort();

        int fileConsistencyFlags = buffer.getInt();


        if(offsetsSize == 8) {
            baseAddress = buffer.getLong();
            long freeFilespaceInfoAddress = buffer.getLong();
            endOfFileAddress = buffer.getLong();
            driverInformationBlockAddress = buffer.getLong();

            // Root Group Symbol Table Entry should start here...
            long linkNameOffset = buffer.getLong();
            rootGroupObjectHeaderAddress = buffer.getLong();
            int cacheType = buffer.getInt();

            if(cacheType == 2) {
                throw new UnsupportedOperationException("Root Group Symbol Table Entry / cacheType = " + cacheType);
            }
            int reserved2 = buffer.getInt();

            byte scratchPad[] = new byte[16];
            buffer.get(scratchPad);

        } else {
            throw new UnsupportedOperationException("offsetsSize = " + offsetsSize);
        }

    }

    private void readVersion2(MappedByteBuffer buffer) throws IOException {
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

    public byte getSuperBlockVersion() {
        return superBlockVersion;
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
