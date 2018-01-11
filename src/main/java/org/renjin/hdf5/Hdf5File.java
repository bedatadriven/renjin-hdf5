package org.renjin.hdf5;


import org.renjin.hdf5.chunked.*;
import org.renjin.hdf5.groups.FractalHeapGroupIndex;
import org.renjin.hdf5.groups.GroupBTree;
import org.renjin.hdf5.groups.GroupIndex;
import org.renjin.hdf5.groups.SimpleGroupIndex;
import org.renjin.hdf5.message.*;
import org.renjin.repackaged.guava.base.Optional;

import java.io.File;
import java.io.IOException;

public class Hdf5File {

    public static final long UNDEFINED_ADDRESS = 0xFFFFFFFFFFFFFFFFL;

    private final Hdf5Data file;
    private final DataObject rootObject;

    public Hdf5File(File file) throws IOException {
        this.file = new Hdf5Data(file);
        this.rootObject = new DataObject(this.file, this.file.getSuperblock().getRootGroupObjectHeaderAddress());
    }

    public DataObject getObject(String... path) throws IOException {

        DataObject node = rootObject;

        for (int i = 0; i < path.length; i++) {
            GroupIndex groupIndex = readGroupIndex(node);
            node = groupIndex.getObject(path[i]);
        }
        return node;
    }

    private GroupIndex readGroupIndex(DataObject object) throws IOException {
        if (object.hasMessage(SymbolTableMessage.class)) {
            SymbolTableMessage symbolTable = object.getMessage(SymbolTableMessage.class);
            return new GroupBTree(this.file, symbolTable);

        } else if(object.hasMessage(LinkInfoMessage.class)) {
            LinkInfoMessage linkInfo = object.getMessage(LinkInfoMessage.class);
            if(linkInfo.hasFractalHeap()) {
                return new FractalHeapGroupIndex(this.file, linkInfo.getFractalHeapAddress());
            } else {
                return new SimpleGroupIndex(this.file, object.getMessages(LinkMessage.class));
            }
        } else {
            throw new UnsupportedOperationException("TODO: cannot construct group index");
        }
    }

    public ChunkIndex openChunkIndex(DataObject object) throws IOException {

        DataspaceMessage dataspace = object.getMessage(DataspaceMessage.class);
        DataLayoutMessage layout = object.getMessage(DataLayoutMessage.class);
        DatatypeMessage datatype = object.getMessage(DatatypeMessage.class);
        Optional<DataStorageMessage> storage = object.getMessageIfPresent(DataStorageMessage.class);

        ChunkDecoderFactory decoderFactory = new ChunkDecoderFactory(file);
        ChunkDecoder chunkDecoder = decoderFactory.create(datatype, layout, storage);

        switch (layout.getChunkIndexingType()) {
            case BTREE:
                return new BTreeChunkIndex(file, layout, chunkDecoder);
            case FIXED_ARRAY:
                return new FixedArrayChunkIndex(file, dataspace, layout, chunkDecoder);

            default:
                throw new UnsupportedOperationException("indexing type: " + layout.getChunkIndexingType());
        }
    }
}
