package org.renjin.hdf5;


import org.renjin.hdf5.chunked.BTreeChunkIndex;
import org.renjin.hdf5.chunked.ChunkIndex;
import org.renjin.hdf5.chunked.FixedArrayChunkIndex;
import org.renjin.hdf5.groups.FractalHeapGroupIndex;
import org.renjin.hdf5.groups.GroupBTree;
import org.renjin.hdf5.groups.GroupIndex;
import org.renjin.hdf5.groups.SimpleGroupIndex;
import org.renjin.hdf5.message.*;

import java.io.File;
import java.io.IOException;

public class Hdf5File {

    public static final long UNDEFINED_ADDRESS = 0xFFFFFFFFFFFFFFFFL;

    private final Hdf5Data file;
    private final GroupIndex groupIndex;

    public Hdf5File(File file) throws IOException {
        this.file = new Hdf5Data(file);

        // Read root group object
        DataObject rootObject = new DataObject(this.file, this.file.getSuperblock().getRootGroupObjectHeaderAddress());
        if (rootObject.hasMessage(SymbolTableMessage.class)) {
            SymbolTableMessage symbolTable = rootObject.getMessage(SymbolTableMessage.class);
            groupIndex = new GroupBTree(this.file, symbolTable);

        } else if(rootObject.hasMessage(LinkMessage.class)) {
            LinkInfoMessage linkInfo = rootObject.getMessage(LinkInfoMessage.class);
            if(linkInfo.hasFractalHeap()) {
                groupIndex = new FractalHeapGroupIndex(this.file, linkInfo.getFractalHeapAddress());
            } else {
                groupIndex = new SimpleGroupIndex(this.file, rootObject.getMessages(LinkMessage.class));
            }
        } else {
            throw new UnsupportedOperationException("TODO: cannot construct group index");
        }
    }

    public DataObject getObject(String name) throws IOException {
        return groupIndex.getObject(name);
    }

    public ChunkIndex openChunkIndex(DataObject object) throws IOException {

        DataspaceMessage dataspace = object.getMessage(DataspaceMessage.class);
        DataLayoutMessage layout = object.getMessage(DataLayoutMessage.class);

        switch (layout.getChunkIndexingType()) {
            case BTREE:
                return new BTreeChunkIndex(file, layout);
            case FIXED_ARRAY:
                return new FixedArrayChunkIndex(file, dataspace, layout);
            default:
                throw new UnsupportedOperationException("indexing type: " + layout.getChunkIndexingType());
        }
    }
}
