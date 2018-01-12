package org.renjin.hdf5.chunked;

import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.HeaderReader;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataspaceMessage;
import org.renjin.repackaged.guava.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The Extensible Array (EA) is a data structure that is used as a chunk index in datasets where the dataspace has a
 * single unlimited dimension. In other words, one dimension is set to H5S_UNLIMITED, and the other dimensions are any
 * number of fixed-size dimensions. The idea behind the extensible array is that a particular data object can be
 * located via a lightweight indexing structure of fixed depth for a given address space. This indexing
 * structure requires only a few (2-3) file operations per element lookup and gives good cache performance.
 * Unlike the B-tree structure, the extensible array is optimized for appends.
 * Where a B-tree would always add at the rightmost node under these circumstances,
 * either creating a deep tree (version 1) or requiring expensive rebalances to correct (version 2),
 * the extensible array has already mapped out a pre-balanced internal structure. This optimized internal
 * structure is instantiated as needed when chunk records are inserted into the structure.
 *
 * An Extensible Array consists of a header, an index block, secondary blocks, data blocks, and (optional)
 * data block pages. The general scheme is that the index block is used to reference a secondary block,
 * which is, in turn, used to reference the data block page where the chunk information is stored.
 * The data blocks will be paged for efficiency when their size passes a threshold value.
 * These pages are laid out contiguously on the disk after the data block,
 * are initialized as needed, and are tracked via bitmaps stored in the secondary block.
 * The number of secondary and data blocks/pages in a chunk index varies as they are allocated as needed
 * and the first few are (conceptually) stored in parent elements as an optimization.
 */
public class ExtensibleArrayChunkIndex extends ChunkIndex {

  private int clientId;

  /**
   * The size in bytes of an element in the Extensible Array.
   */
  private int elementSize;

  /**
   * The number of bits needed to store the maximum number of elements in the Extensible Array.
   */
  private int maxNelementsBits;

  /**
   * The number of elements to store in the index block.
   */
  private int indexBlockElements;

  /**
   * The number of data block pointers to store in the index block.
   */
  private int indexBlockDataPointers;

  /**
   * The minimum number of elements per data block.
   */
  private int dataBlockMinElements;

  /**
   * The minimum number of data block pointers for a secondary block.
   */
  private int secondaryBlockMinDataPointers;

  /**
   * The number of bits needed to store the maximum number of elements in a data block page.
   */
  private int maxDataBlockPageNelmtsBits;


  /**
   * The number of secondary blocks created.
   */
  private long numSecondaryBlocks;

  /**
   * The size of the secondary blocks created.
   */
  private long secondaryBlockSize;

  /**
   * The number of data blocks created.
   */
  private long numDataBlocks;

  /**
   * The size of the data blocks created.
   */
  private long dataBlockSize;

  /**
   * The maximum index set.
   */
  private long maxIndexSet;

  /**
   * The number of elements realized.
   */
  private long numElements;

  /**
   * The address of the index block.
   */
  private long indexBlockAddress;

  private int dimensions;
  private ChunkFactory chunkDecoder;

  /**
   * The checksum for the header.
   */
  private int checksum;
  private long[] dataBlockAddresses;
  private long[] secondaryBlockAddresses;
  private Hdf5Data file;
  private DataspaceMessage dataspace;

  public ExtensibleArrayChunkIndex(Hdf5Data file, DataspaceMessage dataspace, DataLayoutMessage layout, ChunkFactory chunkDecoder) throws IOException {
    this.file = file;
    this.dataspace = dataspace;
    this.dimensions = dataspace.getDimensionality();
    this.chunkDecoder = chunkDecoder;
    readHeader(file, layout);
    readIndex(file);
  }

  private void readHeader(Hdf5Data file, DataLayoutMessage layout) throws IOException {
    int headerSize = 12 + 6 * file.getSuperblock().getLengthSize() + file.getSuperblock().getOffsetSize() + 4;
    HeaderReader reader = file.readerAt(layout.getChunkIndexAddress(), headerSize);
    reader.checkSignature("EAHD");

    int version = reader.readUInt8();
    if(version != 0) {
      throw new UnsupportedOperationException("Version: " + version);
    }
    clientId = reader.readUInt8();
    elementSize = reader.readUInt8();
    maxNelementsBits = reader.readUInt8();

    indexBlockElements = reader.readUInt8();
    dataBlockMinElements = reader.readUInt8();
    secondaryBlockMinDataPointers = reader.readUInt8();
    indexBlockDataPointers = 2 * (secondaryBlockMinDataPointers - 1);
    maxDataBlockPageNelmtsBits = reader.readUInt8();

    numSecondaryBlocks = reader.readLength();
    secondaryBlockSize = reader.readLength();
    numDataBlocks = reader.readLength();
    dataBlockSize = reader.readLength();
    maxIndexSet = reader.readLength();
    numElements = reader.readLength();
    indexBlockAddress = reader.readLength();
    checksum = reader.readInt();
  }

  private void readIndex(Hdf5Data file) throws IOException {
    long indexSize = 6 +                        // signature, version, client id
        file.getSuperblock().getOffsetSize() +  // header address
        indexBlockElements * elementSize +      // elements
        indexBlockDataPointers * file.getSuperblock().getOffsetSize() +
        numSecondaryBlocks * file.getSuperblock().getOffsetSize() +
        4;                                      // checksum

    HeaderReader reader = file.readerAt(indexBlockAddress, indexSize);
    reader.checkSignature("EAIB");
    int version = reader.readUInt8();
    if(version != 0) {
      throw new UnsupportedOperationException("Version: " + version);
    }
    int clientId = reader.readUInt8();
    long headerAddress = reader.readOffset();

    byte[] elements = reader.readBytes(indexBlockElements * elementSize);
    dataBlockAddresses = reader.readOffsets(indexBlockDataPointers);
    secondaryBlockAddresses = reader.readOffsets(Ints.checkedCast(numSecondaryBlocks));
    int checksum = reader.readInt();

  }


  @Override
  public Chunk chunkAt(long[] arrayIndex) throws IOException {
    return readDataBlock(dataBlockAddresses[0]);
  }

  private Chunk readDataBlock(long address) throws IOException {
    ByteBuffer buffer = file.bufferAt(address, dataBlockSize);

    int prefixSize = 6 +  // signature, version, client id
        file.getSuperblock().getOffsetSize() + // header address
        dimensions * file.getSuperblock().getOffsetSize(); // block offset

    HeaderReader reader = new HeaderReader(file.getSuperblock(), buffer);
    reader.checkSignature("EADB");
    int version = reader.readUInt8();
    if(version != 0) {
      throw new UnsupportedOperationException("Version: " + version);
    }
    int clientId = reader.readUInt8();
    long headerAddress = reader.readOffset();
    int[] blockOffset = reader.readIntArray(dimensions);


    throw new UnsupportedOperationException("TODO");
  }
}
