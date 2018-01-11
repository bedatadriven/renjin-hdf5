package org.renjin.hdf5.chunked;

import org.renjin.hdf5.Hdf5Data;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


public class ChunkDecoder {

    private Inflater inf = new Inflater();

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

    /**
     * Buffer for decompressed (inflated) chunk
     */
    private byte[] buffer;


    public ChunkDecoder(int chunkSize) {
        this.chunkSize = chunkSize;
        this.buffer = new byte[chunkSize * 8];
    }

    public double[] read(Hdf5Data file, long address, int size) throws DataFormatException, IOException {

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
        inf.reset();
        inf.setInput(deflatedBufferArray, 0, size);
        int off = 0;
        int len = buffer.length;
        int n;
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

        // Allocate a new array of doubles and decode the uncompressed data
        // into floating point numbers

        double values[] = new double[chunkSize];
        int bi = 0;
        for (int di = 0; di < chunkSize; di++) {
            long longValue =
                (buffer[bi+7] & 0xFFL) << 56
                | (buffer[bi+6] & 0xFFL) << 48
                | (buffer[bi+5] & 0xFFL) << 40
                | (buffer[bi+4] & 0xFFL) << 32
                | (buffer[bi+3] & 0xFFL) << 24
                | (buffer[bi+2] & 0xFFL) << 16
                | (buffer[bi+1] & 0xFFL) << 8
                | (buffer[bi] & 0xFFL);

            values[di] = Double.longBitsToDouble(longValue);
            bi+=8;
        }

        return values;
    }
}
