package org.renjin.hdf5.message;

import org.renjin.hdf5.Flags;
import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

public class DataLayoutMessage extends Message {

    public static final int MESSAGE_TYPE = 0x0008;


    public enum LayoutClass {
        COMPACT,
        CONTIGUOUS,
        CHUNKED,
        VIRTUAL
    }

    public enum ChunkIndexingType {
        SINGLE,
        IMPLICIT,
        FIXED_ARRAY,
        EXTENSIBLE_ARRAY,
        BTREE
    }

    private byte version;
    private LayoutClass layoutClass;
    private long rawAddress;
    private long chunkIndexAddress;
    private int datasetElementSize;
    private int[] dimensionSize;
    private int dimensionality;

    private ChunkIndexingType chunkIndexingType = ChunkIndexingType.BTREE;

    private int maxBits;
    private int indexElements;
    private int minPointers;
    private int minElements;

    /**
     * the number of bits needed to store the maximum number of elements in a data block page.
     */
    private int pageBits;

    public DataLayoutMessage(HeaderReader reader) throws IOException {
        version = reader.readByte();
        if(version == 3) {
            readVersion3(reader);
        } else if(version == 4) {
            readVersion4(reader);
        } else {
            throw new UnsupportedOperationException("Data layout: " + version);
        }
    }

    private void readVersion3(HeaderReader reader) throws IOException {
        layoutClass = LayoutClass.values()[reader.readUInt8()];
        switch (layoutClass) {
            case CHUNKED:
                readChunkedPropertiesV3(reader);
                break;
            default:
                throw new UnsupportedOperationException("Layout class: " + layoutClass);
        }
    }

    private void readChunkedPropertiesV3(HeaderReader reader) throws IOException {
        dimensionality = reader.readUInt8() - 1;
        chunkIndexAddress = reader.readOffset();

        dimensionSize = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSize[i] = reader.readUInt32AsInt();
        }
        datasetElementSize = reader.readUInt32AsInt();
    }


    private void readVersion4(HeaderReader reader) throws IOException {
        layoutClass = LayoutClass.values()[reader.readUInt8()];
        switch (layoutClass) {
            case CHUNKED:
                readChunkedPropertiesV4(reader);
        }
    }

    private void readChunkedPropertiesV4(HeaderReader reader) throws IOException {
        Flags flags = reader.readFlags();
        dimensionality = reader.readUInt8() - 1;
        int dimensionSizeEncodedLength = reader.readUInt8();
        dimensionSize = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSize[i] = (int)reader.readUInt(dimensionSizeEncodedLength);
        }
        datasetElementSize = (int)reader.readUInt(dimensionSizeEncodedLength);

        int chunkIndexingTypeIndex = reader.readUInt8();
        chunkIndexingType = ChunkIndexingType.values()[chunkIndexingTypeIndex - 1];
        switch (chunkIndexingType) {
            case FIXED_ARRAY:
                readFixedArrayProperties(reader);
                break;
            case EXTENSIBLE_ARRAY:
                readExtensibleArrayProperties(reader);
                break;
            default:
                throw new UnsupportedOperationException("chunkIndexingType: " + chunkIndexingType);
        }

        chunkIndexAddress = reader.readOffset();
    }

    private void readFixedArrayProperties(HeaderReader reader) {
        pageBits = reader.readUInt8();
    }

    private void readExtensibleArrayProperties(HeaderReader reader) {
        maxBits = reader.readUInt8();
        indexElements = reader.readUInt8();
        minPointers = reader.readUInt8();
        minElements = reader.readUInt8();
        pageBits = reader.readUInt8();

    }

    public byte getVersion() {
        return version;
    }

    public LayoutClass getLayoutClass() {
        return layoutClass;
    }

    public long getChunkIndexAddress() {
        return chunkIndexAddress;
    }

    public int getDatasetElementSize() {
        return datasetElementSize;
    }

    public int getDimensionality() {
        return dimensionality;
    }

    public int getChunkSize(int dimensionIndex) {
        return dimensionSize[dimensionIndex];
    }

    public ChunkIndexingType getChunkIndexingType() {
        return chunkIndexingType;
    }

    public int[] getChunkSize() {
        return dimensionSize;
    }

    public long getChunkCount() {
        long count = 1;
        for (int i = 0; i < dimensionality; i++) {
            count *= dimensionSize[i];
        }
        return count;
    }

    public int getDimensionSize(int i) {
        return dimensionSize[i];
    }

    public long getChunkElementCount() {
        long count = 1;
        for (int i = 0; i < dimensionality; i++) {
            count *= getChunkSize(i);
        }
        return count;
    }


}
