package org.renjin.hdf5.message;

import org.renjin.hdf5.Flags;
import org.renjin.hdf5.HeaderReader;

import java.io.IOException;

/**
 * The dataspace message describes the number of dimensions (in other words, “rank”) and size of each dimension that
 * the data object has. This message is only used for datasets which have a simple, rectilinear, array-like layout;
 * datasets requiring a more complex layout are not yet supported.
 */
public class DataspaceMessage extends Message {

    public static final int MESSAGE_TYPE = 0x0001;

    public enum Type {
        /**
         * A scalar dataspace; in other words, a dataspace with a single, dimensionless element.
         */
        SCALAR,

        /**
         * A simple dataspace; in other words, a dataspace with a rank greater than 0 and an appropriate number of dimensions.
         */
        SIMPLE,

        /**
         * 	A null dataspace; in other words, a dataspace with no elements.
         */
        NULL
    }

    private final byte version;
    private int dimensionality;

    private long[] dimensionSize;
    private long[] maximumSize;
    private long[] permutationIndex;

    private Type type = Type.SIMPLE;

    public DataspaceMessage(HeaderReader reader) throws IOException {
        version = reader.readByte();
        if(version == 1) {
            readVersion1(reader);
        } else if(version == 2) {
            readVersion2(reader);
        } else {
            throw new UnsupportedOperationException("Dataspace Message Version: " + version);
        }
    }

    private void readVersion1(HeaderReader reader) throws IOException {
        dimensionality = reader.readUInt8();
        Flags flags = reader.readFlags();
        reader.readReserved(1);
        reader.readReserved(4);

        dimensionSize = new long[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSize[i] = reader.readLength();
        }
        if(flags.isSet(0)) {
            readMaximumSize(reader);
        }
        if(flags.isSet(1)) {
            readPermutationIndices(reader);
        }
    }

    private void readVersion2(HeaderReader reader) throws IOException {
        dimensionality = reader.readUInt8();
        Flags flags = reader.readFlags();

        int typeIndex = reader.readUInt8();
        type = Type.values()[typeIndex];

        dimensionSize = new long[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSize[i] = reader.readLength();
        }
        if(flags.isSet(0)) {
            readMaximumSize(reader);
        }
    }


    private void readMaximumSize(HeaderReader reader) throws IOException {
        maximumSize = new long[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            maximumSize[i] = reader.readLength();
        }
    }

    private void readPermutationIndices(HeaderReader reader) throws IOException {
        permutationIndex = new long[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            permutationIndex[i] = reader.readLength();
        }
    }


    public long getTotalElementCount() {
        long count = 1;
        for (int i = 0; i < dimensionality; i++) {
            count *= dimensionSize[i];
        }
        return count;
    }

    public int getDimensionality() {
        return dimensionality;
    }

    public long getDimensionSize(int d) {
        return dimensionSize[d];
    }

    public Type getType() {
        return type;
    }
}
