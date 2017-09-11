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

    public static final int TYPE = 0x0001;
    private final byte version;
    private final int dimensionality;

    private long[] dimensionSize;
    private long[] maximumSize;
    private long[] permutationIndex;

    public DataspaceMessage(HeaderReader reader) throws IOException {
        version = reader.readByte();
        if(version != 1) {
            throw new UnsupportedOperationException("Dataspace Message Version: " + version);
        }
        dimensionality = reader.readUInt8();
        Flags flags = reader.readFlags();
        reader.readReserved(1);
        reader.readReserved(4);

        dimensionSize = new long[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSize[i] = reader.readLength();
        }
        if(flags.isSet(0)) {
            maximumSize = new long[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                maximumSize[i] = reader.readLength();
            }
        }
        if(flags.isSet(1)) {
            permutationIndex = new long[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                permutationIndex[i] = reader.readLength();
            }
        }
    }

}
