package org.renjin.hdf5.chunked;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.io.ByteStreams;
import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.Superblock;
import org.renjin.hdf5.message.DataLayoutMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.InflaterInputStream;

public class ChunkTree {

    private FileChannel file;
    private Superblock superblock;
    private DataLayoutMessage dataLayout;
    private final ChunkNode rootNode;
    private int dim = 0;
    private int chunkCount;

    private Map<Long, ChunkNode> nodes = new HashMap<>();

    private LoadingCache<ChunkKey, Chunk> chunkCache;

    public ChunkTree(FileChannel file, Superblock superblock, DataLayoutMessage dataLayout) throws IOException {
        this.file = file;
        this.superblock = superblock;
        this.dataLayout = dataLayout;
        this.rootNode = readNode(dataLayout.getTreeAddress(), 1000);
        this.dim = dataLayout.getDimensionality();
        this.chunkCount = (int)dataLayout.getChunkElementCount();

        this.chunkCache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(new CacheLoader<ChunkKey, Chunk>() {
                @Override
                public Chunk load(ChunkKey key) throws Exception {
                    return readChunkData(key);
                }
            });

    }

    private ChunkNode getNode(ChunkKey key) throws IOException {
        ChunkNode node = nodes.get(key.getChildPointer());
        if(node == null) {
            node = readNode(key.getChildPointer(), key.getChunkSize());
            nodes.put(key.getChildPointer(), node);
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

    /**
     * Returns the chunk containing the value at the given
     */
    public Chunk chunkAt(long[] arrayIndex) throws IOException {
        return getChunk(arrayIndex);
    }

    private Chunk getChunk(long[] index) throws IOException {
        ChunkKey key = findNode(index);
        try {
            return chunkCache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Chunk readChunkData(ChunkKey key) throws IOException {
        ByteBuffer chunkData = ByteBuffer.allocate(key.getChunkSize());
        file.read(chunkData, key.getChildPointer());
        chunkData.flip();

        byte[] chunkDataArray = chunkData.array();

        InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(chunkDataArray));
        byte[] inflatedArray = ByteStreams.toByteArray(iis);

        DoubleBuffer buffer = ByteBuffer.wrap(inflatedArray).asDoubleBuffer();

        return new Chunk(key, buffer);
    }

    private ChunkKey findNode(long[] chunkCoordinates) throws IOException {

        ChunkNode node = rootNode;
        while(!node.isLeaf()) {
            node = getNode(node.findChildAddress(chunkCoordinates));
        }

        return node.findChildAddress(chunkCoordinates);
    }


}
