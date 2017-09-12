package org.renjin.hdf5.chunked;

import org.renjin.hdf5.HeaderReader;

import java.io.IOException;
import java.util.Arrays;


public class ChunkKey {
    /**
     * Size of chunk in bytes.
     */
    private int chunkSize;

    /**
     * Filter mask, a 32-bit bit field indicating which filters have been skipped for this chunk. Each filter has an
     * index number in the pipeline (starting at 0, with the first filter to apply) and if that filter is skipped, the
     * bit corresponding to its index is set.
     */
    private int filterMask;

    private long offset[];

    private long childPointer;

    public ChunkKey(HeaderReader reader, int dimensionality, boolean hasChildPointer) throws IOException {

        chunkSize = reader.readUInt32AsInt();

        filterMask = reader.readUInt();

        /*
         * The offset of the chunk within the dataset where D is the number of dimensions of the dataset, and the last
         * value is the offset within the dataset’s datatype and should always be zero. For example, if a chunk in a
         * 3-dimensional dataset begins at the position [5,5,5], there will be three such 64-bit values, each with the
         * value of 5, followed by a 0 value.
         */

        offset = new long[dimensionality];
        for (int j = 0; j < dimensionality; j++) {
            offset[j] = reader.readUInt64();
        }
        long zero = reader.readUInt64();

        if(hasChildPointer) {
            this.childPointer = reader.readOffset();
        }
    }

    /**
     * The tree node contains file addresses of subtrees or data depending on the node level.
     *
     * <p>Nodes at Level 0 point to data addresses, either raw data chunks or group nodes. Nodes at non-zero levels
     * point to other nodes of the same B-tree.
     *
     * <p>For raw data chunk nodes, the child pointer is the address of a single raw data chunk. For group nodes,
     * the child pointer points to a symbol table, which contains information for multiple symbol table entries.
     */
    public long getChildPointer() {
        return childPointer;
    }

    /**
     *
     * @return the size of the chunk, in bytes.
     */
    public int getChunkSize() {
        return chunkSize;
    }


    /**
     * Compares this chunk's offset with the given index.
     */
    public int compare(long[] index) {
        for (int i = 0; i < offset.length; i++) {
            if(offset[i] != index[i]) {
                return Long.compare(offset[i], index[i]);
            }
        }
        return 0;
    }

    /**
     * 	The offset of the chunk within the dataset where D is the number of dimensions of the dataset.
     * 	For example, if a chunk in a 3-dimensional dataset begins at the position [5,5,5], there will be three
     * 	such 64-bit values, each with the value of 5.
     */
    public long[] getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("ChunkKey{");
        for (int i = 0; i < offset.length; i++) {
            if (i > 0) {
                s.append(",");
            }
            s.append(offset[i]);
        }
        s.append("}");
        return s.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChunkKey key = (ChunkKey) o;

        return Arrays.equals(offset, key.offset);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(offset);
    }
}
