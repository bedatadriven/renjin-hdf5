package org.renjin.hdf5;


import org.renjin.hdf5.chunked.ChunkIndex;
import org.renjin.hdf5.chunked.BTreeChunkIndex;
import org.renjin.hdf5.chunked.FixedArrayChunkIndex;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataspaceMessage;
import org.renjin.hdf5.message.LinkInfoMessage;
import org.renjin.hdf5.message.LinkMessage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class Hdf5File {

    public static final long UNDEFINED_ADDRESS = 0xFFFFFFFFFFFFFFFFL;

    private final FileChannel channel;
    private final Superblock superblock;
    private final DataObject rootObject;

    public Hdf5File(File file) throws IOException {
        channel = new RandomAccessFile(file, "r").getChannel();
        superblock = new Superblock(channel);

        System.out.println(((double) channel.size()) / (double)Integer.MAX_VALUE);

        // Read root group object
        rootObject = new DataObject(channel, superblock, superblock.getRootGroupObjectHeaderAddress());
    }

    public List<String> getRootObjects() {
        List<String> names = new ArrayList<>();
        Iterable<LinkMessage> messages = rootObject.getMessages(LinkMessage.class);
        for (LinkMessage message : messages) {
            names.add(message.getLinkName());
        }
        return names;
    }

    public DataObject getObject(String name) throws IOException {

        LinkInfoMessage linkInfo = rootObject.getMessage(LinkInfoMessage.class);
        if(linkInfo.hasFractalHeap()) {
            FractalHeap heap = new FractalHeap(this.channel, superblock, linkInfo.getFractalHeapAddress());
            FractalHeap.DirectBlock rootBlock = heap.getRootBlock();
            LinkMessage link = rootBlock.readLinkMessage();

            if(link.getLinkName().equals(name)) {
                return objectAt(link.getAddress());
            }
        } else {

            Iterable<LinkMessage> messages = rootObject.getMessages(LinkMessage.class);
            for (LinkMessage message : messages) {
                if (message.getLinkName().equals(name)) {
                    return objectAt(message.getAddress());
                }
            }
        }
        throw new IllegalArgumentException("No such link: " + name);
    }

    private DataObject objectAt(long address) throws IOException {
        System.out.println("Address: " + Long.toHexString(address));
        return new DataObject(channel, superblock, address);
    }

    public ChunkIndex openChunkIndex(DataObject object) throws IOException {

        DataspaceMessage dataspace = object.getMessage(DataspaceMessage.class);
        DataLayoutMessage layout = object.getMessage(DataLayoutMessage.class);

        switch (layout.getChunkIndexingType()) {
            case BTREE:
                return new BTreeChunkIndex(channel, superblock, layout);
            case FIXED_ARRAY:
                return new FixedArrayChunkIndex(channel ,superblock, dataspace, layout);
            default:
                throw new UnsupportedOperationException("indexing type: " + layout.getChunkIndexingType());
        }
    }
}
