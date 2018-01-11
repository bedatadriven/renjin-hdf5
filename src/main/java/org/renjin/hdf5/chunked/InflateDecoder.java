package org.renjin.hdf5.chunked;

import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.repackaged.guava.primitives.Ints;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


public class InflateDecoder implements ChunkDecoder {

    private Inflater inf = new Inflater();

    private Hdf5Data file;

    /**
     * The number of data *elements* (not bytes) in each chunk.
     */
    private int chunkSize;


    /**
     * Array backing the buffer for compressed data read in from the file.
     */
    private byte[] deflatedBufferArray;

    /**
     * The buffer for compressed data read in from the file.
     */
    private ByteBuffer deflatedBuffer;

    private final ChunkFactory chunkFactory;

    private final int chunkSizeBytes;

    public InflateDecoder(Hdf5Data file, DataLayoutMessage dataLayout, ChunkFactory chunkFactory) {
        this.file = file;
        this.chunkSize = Ints.checkedCast(dataLayout.getChunkElementCount());
        this.chunkFactory = chunkFactory;
        this.chunkSizeBytes = this.chunkSize * dataLayout.getDatasetElementSize();
    }


    @Override
    public Chunk read(long[] chunkOffset, long address, int size) throws IOException {

        // Set up our buffer for reading in the compressed data
        if(deflatedBufferArray == null || deflatedBufferArray.length < size) {
            deflatedBufferArray = new byte[size];
            deflatedBuffer = ByteBuffer.wrap(deflatedBufferArray);
        }

        // Read the compressed chunk into our buffer
        deflatedBuffer.position(0);
        deflatedBuffer.limit(size);
        file.read(deflatedBuffer, address);

        // Deflate from the compressed buffer into the uncompressed buffer
        byte[] buffer = new byte[chunkSizeBytes];
        inf.reset();
        inf.setInput(deflatedBufferArray, 0, size);
        int off = 0;
        int len = buffer.length;
        int n;
        try {
            while ((n = inf.inflate(buffer, off, len)) == 0) {
                if (inf.finished() || inf.needsDictionary()) {
                    break;
                }
                if (inf.needsInput()) {
                    throw new EOFException("Unexpected end of deflated chunk.");
                }
                off += n;
                len -= n;
            }
        } catch (DataFormatException e) {
            throw new IOException(e);
        }

        return chunkFactory.wrap(chunkOffset, ByteBuffer.wrap(buffer));
//
//        // Allocate a new array of doubles and decode the uncompressed data
//        // into floating point numbers
//
//        double values[] = new double[chunkSize];
//        int bi = 0;
//        for (int di = 0; di < chunkSize; di++) {
//            long longValue =
//                (buffer[bi+7] & 0xFFL) << 56
//                | (buffer[bi+6] & 0xFFL) << 48
//                | (buffer[bi+5] & 0xFFL) << 40
//                | (buffer[bi+4] & 0xFFL) << 32
//                | (buffer[bi+3] & 0xFFL) << 24
//                | (buffer[bi+2] & 0xFFL) << 16
//                | (buffer[bi+1] & 0xFFL) << 8
//                | (buffer[bi] & 0xFFL);
//
//            values[di] = Double.longBitsToDouble(longValue);
//            bi+=8;
//        }
//
//        return new DoubleChunk()DoubleBuffer.wrap(values);
    }
}
