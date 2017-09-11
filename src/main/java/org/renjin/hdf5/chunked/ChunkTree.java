package org.renjin.hdf5.chunked;

import com.google.common.io.ByteStreams;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLong;
import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.Superblock;
import org.renjin.hdf5.message.DataLayoutMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class ChunkTree {

    private FileChannel file;
    private Superblock superblock;
    private DataLayoutMessage dataLayout;
    private final ChunkNode rootNode;
    private int dim = 0;

    private Map<Long, ChunkNode> nodes = new HashMap<>();

    public ChunkTree(FileChannel file, Superblock superblock, DataLayoutMessage dataLayout) throws IOException {
        this.file = file;
        this.superblock = superblock;
        this.dataLayout = dataLayout;
        this.rootNode = readNode(dataLayout.getTreeAddress(), 1000);
        this.dim = dataLayout.getDimensionality();
    }

    private ChunkNode getNode(ChunkKey key) throws IOException {
        ChunkNode node = nodes.get(key.getAddress());
        if(node == null) {
            node = readNode(key.getAddress(), key.getChunkSize());
            nodes.put(key.getAddress(), node);
        }
        return node;
    }

    private ChunkNode readNode(long address, int size) throws IOException {

        ByteBuffer nodeBuffer = ByteBuffer.allocate(size);
        int bytesRead = file.read(nodeBuffer, address);
        assert bytesRead == size;

        nodeBuffer.flip();

        return new ChunkNode(dataLayout, new HeaderReader(superblock, nodeBuffer));
    }

    public double valueAt(int[] indexes) throws IOException {
        long chunkCoordinates[] = new long[dim];
        for (int i = 0; i < dim; i++) {
            chunkCoordinates[i] = indexes[i] / dataLayout.getChunkSize(i);
        }

        ChunkKey key = findNode(chunkCoordinates);

        ByteBuffer chunkData = ByteBuffer.allocate(key.getChunkSize());
        file.read(chunkData, key.getAddress());
        chunkData.flip();

        byte[] chunkDataArray = chunkData.array();
        InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(chunkDataArray));
        LittleEndianDataInputStream lidis = new LittleEndianDataInputStream(iis);

        for (int i = 0; i < 30; i++) {
            System.out.println(lidis.readDouble());
        }

        throw new UnsupportedOperationException("TODO");
    }

    private ChunkKey findNode(long[] chunkCoordinates) throws IOException {

        ChunkNode node = rootNode;
        while(!node.isLeaf()) {
            node = getNode(node.findChildAddress(chunkCoordinates));
        }

        return node.findChildAddress(chunkCoordinates);
    }


}
