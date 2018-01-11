package org.renjin.hdf5.chunked;


import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataStorageMessage;
import org.renjin.repackaged.guava.cache.CacheBuilder;
import org.renjin.repackaged.guava.cache.CacheLoader;
import org.renjin.repackaged.guava.cache.LoadingCache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.DataFormatException;

public class BTreeChunkIndex extends ChunkIndex {

    private Hdf5Data file;
    private DataLayoutMessage dataLayout;
    private final ChunkDecoder chunkDecoder;
    private final ChunkNode rootNode;

    private Map<Long, ChunkNode> nodes = new HashMap<>();

    private LoadingCache<ChunkKey, Chunk> chunkCache;

    public BTreeChunkIndex(Hdf5Data file, DataLayoutMessage dataLayout, ChunkDecoder decoder) throws IOException {
        this.file = file;
        this.dataLayout = dataLayout;
        this.rootNode = readNode(dataLayout.getChunkIndexAddress(), 4096);
        this.chunkDecoder = decoder;
        this.chunkCache = CacheBuilder.newBuilder()
            .softValues()
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
        return new ChunkNode(dataLayout, file.readerAt(address, size));
    }

    /**
     * Returns the chunk containing the value at the given
     */
    @Override
    public Chunk chunkAt(long[] arrayIndex) throws IOException {
        assert dataLayout.getDimensionality() == arrayIndex.length : "Invalid dimensionality";
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
        return chunkDecoder.read(key.getOffset(), key.getChildPointer(), key.getChunkSize());
    }

    private ChunkKey findNode(long[] chunkCoordinates) throws IOException {

        ChunkNode node = rootNode;
        while(!node.isLeaf()) {
            node = getNode(node.findChildAddress(chunkCoordinates));
        }

        return node.findChildAddress(chunkCoordinates);
    }


}
