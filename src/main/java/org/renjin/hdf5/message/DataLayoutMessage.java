package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

public class DataLayoutMessage extends Message {

    public static final int MESSAGE_TYPE = 0x0008;

    public enum LayoutClass {
        COMPACT,
        CONTIGUOUS,
        CHUNKED
    }
    private final byte version;
    private final LayoutClass layoutClass;
    private long rawAddress;
    private long treeAddress;
    private int datasetElementSize;
    private int[] dimensionSize;
    private int dimensionality;

    public DataLayoutMessage(HeaderReader reader) throws IOException {
        version = reader.readByte();
        if(version != 3) {
            throw new UnsupportedOperationException("Data layout: " + version);
        }
        layoutClass = LayoutClass.values()[reader.readUInt8()];
        switch (layoutClass) {
            case CHUNKED:
                readChunkedProperties(reader);
                break;
            default:
                throw new UnsupportedOperationException("Layout class: " + layoutClass);
        }
    }

    private void readChunkedProperties(HeaderReader reader) throws IOException {
        dimensionality = reader.readUInt8() - 1;
        treeAddress = reader.readOffset();

        dimensionSize = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSize[i] = reader.readUInt32AsInt();
        }
        datasetElementSize = reader.readUInt32AsInt();
    }

    public byte getVersion() {
        return version;
    }

    public LayoutClass getLayoutClass() {
        return layoutClass;
    }

    public long getTreeAddress() {
        return treeAddress;
    }

    public int getDatasetElementSize() {
        return datasetElementSize;
    }

    public int[] getDimensionSize() {
        return dimensionSize;
    }

    public int getDimensionality() {
        return dimensionality;
    }

    public int getChunkSize(int dimensionIndex) {
        return dimensionSize[dimensionIndex];
    }

    public int[] getChunkSize() {
        return dimensionSize;
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
