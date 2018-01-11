package org.renjin.hdf5.chunked;

import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataStorageMessage;
import org.renjin.hdf5.message.DatatypeMessage;
import org.renjin.hdf5.message.Filter;
import org.renjin.repackaged.guava.base.Optional;
import org.renjin.repackaged.guava.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ChunkDecoderFactory {

  private Hdf5Data file;

  public ChunkDecoderFactory(Hdf5Data file) {
    this.file = file;
  }

  public ChunkDecoder create(DatatypeMessage datatype, DataLayoutMessage dataLayout, Optional<DataStorageMessage> dataStorage) {

    ChunkFactory factory = createFactory(datatype);

    ArrayList<Filter> filters = new ArrayList<>();
    if(dataStorage.isPresent()) {
      filters.addAll(dataStorage.get().getFilters());
    }

    if(filters.isEmpty()) {
      return new UncompressedDecoder(file, factory);

    } else if(filters.size() == 1) {
      switch (filters.get(0).getFilterId()) {
        case Filter.FILTER_DEFLATE:
          return new InflateDecoder(file, dataLayout, factory);
      }
      throw new UnsupportedOperationException("Filter: " + filters.get(0).getFilterId());

    } else {
      throw new UnsupportedOperationException("Filters: " + filters);
    }
  }

  private ChunkFactory createFactory(DatatypeMessage datatype) {
    if(datatype.isDoubleIEE754()) {
      return new ChunkFactory() {
        @Override
        public Chunk wrap(long[] chunkOffset, ByteBuffer buffer) {
          return new DoubleChunk(chunkOffset, buffer.asDoubleBuffer());
        }
      };

    } else if(datatype.isSignedInteger32()) {
      return new ChunkFactory() {
        @Override
        public Chunk wrap(long[] chunkOffset, ByteBuffer buffer) {
          return new Int32Chunk(chunkOffset, buffer.asIntBuffer());
        }
      };
    }

    throw new UnsupportedOperationException("Datatype: " + datatype);
  }
}
