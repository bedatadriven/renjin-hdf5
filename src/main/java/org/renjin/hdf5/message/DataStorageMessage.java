package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;
import org.renjin.repackaged.guava.base.Charsets;

import java.util.ArrayList;
import java.util.List;

/**
 * This message describes the filter pipeline which should be applied to the data stream by providing filter
 * identification numbers, flags, a name, and client data.
 *
 * <p>This message may be present in the object headers of both dataset and group objects.
 * For datasets, it specifies the filters to apply to raw data. For groups, it specifies the filters to apply to the
 * group’s fractal heap. Currently, only datasets using chunked data storage use the filter pipeline on their raw data.</p>
 */
public class DataStorageMessage extends Message {

    public static final int MESSAGE_TYPE = 0x000B;

    private final List<Filter> filters = new ArrayList<>();

    public DataStorageMessage(HeaderReader reader) {
        byte version = reader.readByte();
        if(version == 1) {
            readVersion1(reader);
        } else if(version == 2) {
            readVersion2(reader);
        } else {
            throw new UnsupportedOperationException("version: " + version);
        }
    }

    private void readVersion1(HeaderReader reader) {
        int numFilters = reader.readUInt8();
        reader.readReserved(2);
        reader.readReserved(4);

        for (int i = 0; i < numFilters; i++) {
            int filterId = reader.readUInt16();

            /*
             * Each filter has an optional null-terminated ASCII name and this field holds the length of the name
             * including the null termination padded with nulls to be a multiple of eight. If the filter has
             * no name then a value of zero is stored in this field.
             */
            int nameLength = reader.readUInt16();

            int flags = reader.readUInt16();
            boolean optional = (flags & 0x1) != 0;

            int numClientDataValues = reader.readUInt16();

            String name = null;
            if(nameLength != 0) {
                name = reader.readNullTerminatedAsciiString(nameLength);
            }

            int[] clientData = reader.readIntArray(numClientDataValues);

            /*
             * Four bytes of zeroes are added to the message at this point if the Client Data Number of
             * Values field contains an odd number.
             */
            if(numClientDataValues % 2 != 0) {
                reader.readReserved(4);
            }

            filters.add(new Filter(filterId, name, clientData, optional));
        }
    }

    private void readVersion2(HeaderReader reader) {
        int numFilters = reader.readUInt8();

        for (int i = 0; i < numFilters; i++) {
            int filterId = reader.readUInt16();

            /*
             * Each filter has an optional null-terminated ASCII name and this field holds the length of the name
             * including the null termination padded with nulls to be a multiple of eight. If the filter has no name
             * then a value of zero is stored in this field.
             */
            int nameLength = 0;

            /*
             * Filters with IDs less than 256 (in other words, filters that are defined in this format documentation)
             * do not store the Name Length or Name fields.
             */
            if(filterId >= 256) {
                nameLength = reader.readUInt16();
            }

            int flags = reader.readUInt16();
            boolean optional = (flags & 0x1) != 0;

            int numClientDataValues = reader.readUInt16();

            String name = null;
            if(nameLength != 0) {
                reader.readString(nameLength, Charsets.US_ASCII);
            }

            int[] clientData = reader.readIntArray(numClientDataValues);

            filters.add(new Filter(filterId, name, clientData, optional));
        }
    }

    public List<Filter> getFilters() {
        return filters;
    }
}
