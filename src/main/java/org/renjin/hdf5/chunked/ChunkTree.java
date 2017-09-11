package org.renjin.hdf5.chunked;

import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.Superblock;
import org.renjin.hdf5.message.DataLayoutMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

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
        this.rootNode = readNode(dataLayout.getTreeAddress());
        this.dim = dataLayout.getDimensionality();
    }

    private ChunkNode getNode(long address) throws IOException {
        ChunkNode node = nodes.get(address);
        if(node == null) {
            node = readNode(address);
            nodes.put(address, node);
        }
        return node;
    }

    private ChunkNode readNode(long address) throws IOException {

        ByteBuffer nodeBuffer = ByteBuffer.allocate(1000);
        int bytesRead = file.read(nodeBuffer, address);

        nodeBuffer.flip();

        return new ChunkNode(dataLayout, new HeaderReader(superblock, nodeBuffer));
    }

    private double valueAt(int[] indexes) {
        int chunkCoordinates[] = new int[dim];
        for (int i = 0; i < dim; i++) {
            chunkCoordinates[i] = indexes[i] / dataLayout.getChunkSize(i);
        }

        ChunkNode node = findNode(chunkCoordinates);

        throw new UnsupportedOperationException("TODO");
    }

    private ChunkNode findNode(int[] chunkCoordinates) {
        throw new UnsupportedOperationException("TODO");
    }


}
