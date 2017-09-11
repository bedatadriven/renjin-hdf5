package org.renjin.hdf5;


import org.renjin.hdf5.chunked.ChunkTree;
import org.renjin.hdf5.message.DataLayoutMessage;
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
        Iterable<LinkMessage> messages = rootObject.getMessages(LinkMessage.class);
        for (LinkMessage message : messages) {
            if(message.getLinkName().equals(name)) {
                return objectAt(message.getAddress());
            }
        }
        throw new IllegalArgumentException("No such link: " + name);
    }

    private DataObject objectAt(long address) throws IOException {
        System.out.println("Address: " + Long.toHexString(address));
        return new DataObject(channel, superblock, address);
    }

    public ChunkTree openChunkTree(DataLayoutMessage layout) throws IOException {
        return new ChunkTree(channel, superblock, layout);
    }
}
