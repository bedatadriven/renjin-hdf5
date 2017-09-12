package org.renjin.hdf5;

import org.renjin.hdf5.message.LinkMessage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class FractalHeap {
    private FileChannel channel;
    private Superblock superblock;
    private final byte version;
    private final int heapIdLength;
    private final long maximumSizeOfManagedObjects;
    private final long rootBlockAddress;
    private final long currentNumOfRowsInRootIndirectBlock;
    private final int startNumOfRowsInRootIndirectBlock;
    private final long startingBlockSize;
    private final int tableWidth;
    private final long nextHugeObjectId;
    private final long btreeAddressOfHugeObjects;
    private final long amountOfFreeSpaceInManagedBlocks;
    private final long addressOfManagedBlockFreeSpaceManager;
    private final long amountOfManagedSpaceInHeap;
    private final long amountOfAllocatedManagedSpaceInHeap;
    private final long offsetOfDirectBlockAllocationIteratorInManagedSpace;
    private final long numberOfManagedObjectsInHeap;
    private final long sizeOfHugeObjectsInHeap;
    private final long numberOfHugeObjectsInHeap;
    private final long sizeOfTinyObjectsInHeap;
    private final long numberOfTinyObjectsInHeap;
    private final long maximumDirectBlockSize;
    private final int maximumHeapSize;
    private final Flags headerFlags;

    public FractalHeap(FileChannel channel, Superblock superblock, long address) throws IOException {
        this.channel = channel;
        this.superblock = superblock;

        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, address,
            Math.min(maxHeaderSize(superblock), channel.size() - address));

        HeaderReader reader = new HeaderReader(superblock, mappedByteBuffer);
        reader.checkSignature("FRHP");

        version = reader.readByte();
        heapIdLength = reader.readUInt16();
        int ioFiltersEncodedLength = reader.readUInt16();
        headerFlags = reader.readFlags();
        maximumSizeOfManagedObjects = reader.readUInt32();
        nextHugeObjectId = reader.readLength();
        btreeAddressOfHugeObjects = reader.readOffset();
        amountOfFreeSpaceInManagedBlocks = reader.readLength();
        addressOfManagedBlockFreeSpaceManager = reader.readOffset();
        amountOfManagedSpaceInHeap = reader.readLength();
        amountOfAllocatedManagedSpaceInHeap = reader.readLength();
        offsetOfDirectBlockAllocationIteratorInManagedSpace = reader.readLength();
        numberOfManagedObjectsInHeap = reader.readLength();
        sizeOfHugeObjectsInHeap = reader.readLength();
        numberOfHugeObjectsInHeap = reader.readLength();
        sizeOfTinyObjectsInHeap = reader.readLength();
        numberOfTinyObjectsInHeap = reader.readLength();
        tableWidth = reader.readUInt16();
        startingBlockSize = reader.readLength();
        maximumDirectBlockSize = reader.readLength();
        maximumHeapSize = reader.readUInt16();
        startNumOfRowsInRootIndirectBlock = reader.readUInt16();
        rootBlockAddress = reader.readOffset();
        currentNumOfRowsInRootIndirectBlock = reader.readUInt16();
        if(ioFiltersEncodedLength > 0) {
            long sizeOfFilteredRootDirectBlock = reader.readLength();
            long ioFilterMask = reader.readUInt();
            byte[] ioFilterInformation = reader.readBytes(ioFiltersEncodedLength);
        }
        int checkSum = reader.readUInt();
    }

    private boolean isDirectBlockChecksummed() {
        return headerFlags.isSet(1);
    }

    public static long maxHeaderSize(Superblock superblock) {
        return
             4 +  // Signature
             1 +  // Version
             4 +  // Heap ID Length + I/O filters encoded length
             1 +  // Flags
             4 +  // Max size of managed objects
            13 * superblock.getLengthSize() +   // length fields
             3 * superblock.getOffsetSize() +   // offset fields
             2 + // table width
             4 + // max heap size + start # fo rows
             2 + // current row
             4 + // i/o filter mask
           100 + // I/O filter information ??
             4; // checksum

    }


    /**
     * The number of bytes used to encode this field is the Maximum Heap Size (in the heap’s header) divided by
     * 8 and rounded up to the next highest integer, for values that are not a multiple of 8. This value is
     * principally used for file integrity checking.
     */
    private int blockOffsetSize() {
        int bytes = maximumHeapSize / 8;
        if(maximumHeapSize % 8 != 0) {
            bytes++;
        }
        return bytes;
    }

    public DirectBlock getRootBlock() throws IOException {
        return new DirectBlock(rootBlockAddress, startingBlockSize);
    }

    public class DirectBlock {

        private final MappedByteBuffer buffer;

        public DirectBlock(long address, long size) throws IOException {

            buffer = channel.map(FileChannel.MapMode.READ_ONLY, address, size);
            HeaderReader reader = new HeaderReader(superblock, buffer);

            reader.checkSignature("FHDB");
            byte version = reader.readByte();
            if(version != 0) {
                throw new IOException("Direct block version " + version);
            }

            long heapHeaderAddress = reader.readOffset();
            long blockOffset = reader.readUInt(blockOffsetSize());
            if(isDirectBlockChecksummed()) {
                int checkSum = reader.readUInt();
            }
        }

        public LinkMessage readLinkMessage() throws IOException {
            return new LinkMessage(new HeaderReader(superblock, buffer.slice()));
        }

    }

}
