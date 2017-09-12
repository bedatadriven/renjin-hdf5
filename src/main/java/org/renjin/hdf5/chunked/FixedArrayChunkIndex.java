package org.renjin.hdf5.chunked;

import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.Superblock;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataspaceMessage;
import org.renjin.repackaged.guava.cache.Cache;
import org.renjin.repackaged.guava.cache.CacheBuilder;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * The Fixed Array index can be used when the dataset has fixed maximum dimension sizes.
 *
 * <p>Since the maximum number of chunks is known, an array of in-file-on-disk addresses based on the maximum number of
 * chunks is allocated when data is written to the dataset. To access a dataset chunk with a specified offset,
 * the chunk index associated with the offset is calculated. The index is mapped into the array to locate the disk
 * address for the chunk.
 *
 * <p>The Fixed Array (FA) index structure provides space and speed improvements in locating chunks over
 * index structures that handle more dynamic data accesses like a Version 2 B-tree index. The entry into the Fixed Array
 * is the Fixed Array header which contains metadata about the entries stored in the array. The header contains a
 * pointer to a data block which stores the array of entries that describe the dataset chunks. For greater efficiency,
 * the array will be divided into multiple pages if the number of entries exceeds a threshold value.
 * The space for the data block and possibly data block pages are allocated as a single contiguous block of space.
 *
 * The content of the data block depends on whether paging is activated or not. When paging is not used,
 * elements that describe the chunks are stored in the data block. If paging is turned on, the data block
 * contains a bitmap indicating which pages are initialized. Then subsequent data block pages will contain
 * the entries that describe the chunks.
 */
public class FixedArrayChunkIndex extends ChunkIndex {

    private static final int CHECKSUM_SIZE = 4;

    private FileChannel channel;
    private final Superblock superblock;
    private final DataspaceMessage dataspace;
    private final DataLayoutMessage layout;
    private final long maxNumEntries;
    private final int dataBlockHeaderSize;
    private MappedByteBuffer dataBlockBuffer;
    private final byte entrySize;

    private int[] chunkSize;
    private int[] chunkDims;
    private final int numberElementsPerDataBlockPage;
    private final int numberOfPages;
    private final int dataBlockPageSize;

    private final Cache<Integer, Chunk> chunkCache;

    public FixedArrayChunkIndex(FileChannel channel, Superblock superblock,
                                DataspaceMessage dataspace,
                                DataLayoutMessage layout) throws IOException {
        this.channel = channel;
        this.superblock = superblock;
        this.dataspace = dataspace;
        this.layout = layout;
        MappedByteBuffer headerBuffer = channel.map(FileChannel.MapMode.READ_ONLY,
            layout.getChunkIndexAddress(), headerSize(superblock));
        HeaderReader reader = new HeaderReader(superblock, headerBuffer);

        reader.checkSignature("FAHD");
        byte version = reader.readByte();
        if(version != 0) {
            throw new UnsupportedOperationException("FAHD version: " + version);
        }
        byte clientId = reader.readByte();
        if(clientId != 0) {
            throw new UnsupportedOperationException("client id: " + clientId);
        }
        entrySize = reader.readByte();
        byte pageBits = reader.readByte();
        maxNumEntries = reader.readLength();

        numberElementsPerDataBlockPage = (1 << pageBits);
        numberOfPages = ceilDiv(maxNumEntries, numberElementsPerDataBlockPage);
        dataBlockPageSize = numberElementsPerDataBlockPage * entrySize + CHECKSUM_SIZE;

        long dataBlockAddress = reader.readOffset();
        int checkSum = reader.readInt();

        int pagingBitMapSize = ceilDiv(numberOfPages, 8);
        dataBlockHeaderSize = 6 + superblock.getOffsetSize() + pagingBitMapSize + CHECKSUM_SIZE;

        long datablockSize = dataBlockHeaderSize +
            dataBlockPageSize * numberOfPages;

        dataBlockBuffer = channel.map(FileChannel.MapMode.READ_ONLY, dataBlockAddress, datablockSize);
        dataBlockBuffer.order(ByteOrder.LITTLE_ENDIAN);

        HeaderReader dataBlockHeaderReader = new HeaderReader(superblock, dataBlockBuffer);
        dataBlockHeaderReader.checkSignature("FADB");
        byte dataBlockVersion = dataBlockHeaderReader.readByte();
        byte dataBlockClientId = dataBlockHeaderReader.readByte();
        long dataBlockHeaderAddress = dataBlockHeaderReader.readOffset();
        byte[] pagingBitmask = dataBlockHeaderReader.readBytes(pagingBitMapSize);
        int dataBlockHeaderChecksum = dataBlockHeaderReader.readInt();

        chunkSize = layout.getChunkSize();
        chunkDims = new int[layout.getDimensionality()];
        for (int i = 0; i < layout.getDimensionality(); i++) {
            chunkDims[i] = ceilDiv(dataspace.getDimensionSize(i), layout.getChunkSize(i));
        }

        this.chunkCache = CacheBuilder.newBuilder()
            .softValues()
            .build();
    }

    private int ceilDiv(long dimensionSize, int chunkSize) {
        int count = (int)(dimensionSize / chunkSize);
        if(dimensionSize % chunkSize != 0) {
            count = count + 1;
        }
        return count;
    }

    private static int headerSize(Superblock superblock) {
        return  4 + // signature
                4 + // version + client id ...
                superblock.getLengthSize() + // max num entries
                superblock.getOffsetSize() + // data block adddres
                4; // checksum
    }

    @Override
    public Chunk chunkAt(long[] arrayIndex) throws IOException {
        int chunkIndex = arrayIndexToChunkIndex(arrayIndex);

        Chunk chunk = chunkCache.getIfPresent(chunkIndex);
        if(chunk == null) {
            int pageIndex = chunkIndex / numberElementsPerDataBlockPage;
            int addressOffset =
                dataBlockHeaderSize +
                    pageIndex * dataBlockPageSize +
                    (chunkIndex % numberElementsPerDataBlockPage) * entrySize;

            long chunkAddress;
            if (entrySize == 8) {
                chunkAddress = dataBlockBuffer.getLong(addressOffset);
            } else {
                throw new UnsupportedOperationException("entrySize: " + entrySize);
            }

            chunk = readChunk(arrayIndex, chunkAddress);
            chunkCache.put(chunkIndex, chunk);
        }
        return chunk;
    }

    private Chunk readChunk(long[] arrayIndex, long chunkAddress) throws IOException {

        long chunkOffset[] = Arrays.copyOf(arrayIndex, arrayIndex.length);
        for (int i = 0; i < chunkOffset.length; i++) {
            chunkOffset[i] = (chunkOffset[i] / chunkSize[i]) * chunkSize[i];
        }

        MappedByteBuffer chunkBuffer = channel.map(FileChannel.MapMode.READ_ONLY, chunkAddress,
            layout.getChunkElementCount() * layout.getDatasetElementSize());

        chunkBuffer.order(ByteOrder.LITTLE_ENDIAN);

        double[] values = new double[(int)layout.getChunkElementCount()];
        chunkBuffer.asDoubleBuffer().get(values);

        return new Chunk(chunkOffset, values);
    }

    public int arrayIndexToChunkIndex(long arrayIndex[]) {
        long chunkIndex = 0;
        long offset = 1;
        for(int i=chunkDims.length-1;i>=0;i--) {
            long chunkOffset = arrayIndex[i] / chunkSize[i];
            chunkIndex += chunkOffset * offset;
            offset *= chunkDims[i];
        }
        return (int)chunkIndex;
    }
}
