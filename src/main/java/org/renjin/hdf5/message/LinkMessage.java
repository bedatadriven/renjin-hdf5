package org.renjin.hdf5.message;

import com.google.common.base.Charsets;
import org.renjin.hdf5.Flags;
import org.renjin.hdf5.HeaderReader;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * This message encodes the information for a link in a group’s object header, when the group is storing its
 * links “compactly”, or in the group’s fractal heap, when the group is storing its links “densely”.
 *
 * A group is storing its links compactly when the fractal heap address in the Link Info Message is
 * set to the “undefined address” value.
 */
public class LinkMessage extends Message {

    public static final int TYPE = 0x6;

    public static final byte HARD_LINK = 0;
    public static final byte SOFT_LINK = 1;
    public static final byte EXTERNAL = 64;

    private byte version;
    private byte linkType;
    private long creationOrder;
    private Charset charset;
    private long address;
    private final String linkName;

    public LinkMessage(HeaderReader reader) throws IOException {
        version = reader.readByte();
        Flags flags = reader.readFlags();

        if (flags.isSet(3)) {
            linkType = reader.readByte();
        }

        if (flags.isSet(2)) {
            creationOrder = reader.readUInt64();
        }

        charset = Charsets.US_ASCII;
        if (flags.isSet(4)) {
            byte charsetIndex = reader.readByte();
            switch (charsetIndex) {
                case 0:
                    charset = Charsets.US_ASCII;
                    break;
                case 1:
                    charset = Charsets.UTF_8;
                    break;
            }
        }

        int linkNameLength = reader.readVariableLengthSizeAsInt(flags);
        linkName = reader.readString(linkNameLength, charset);

        switch (linkType) {
            case HARD_LINK:
                address = reader.readOffset();
                break;
        }
    }

    public String getLinkName() {
        return linkName;
    }

    public byte getLinkType() {
        return linkType;
    }

    public long getAddress() {
        return address;
    }
}
