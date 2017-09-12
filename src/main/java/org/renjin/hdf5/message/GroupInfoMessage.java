package org.renjin.hdf5.message;

import org.renjin.hdf5.Flags;
import org.renjin.hdf5.HeaderReader;

public class GroupInfoMessage extends Message {

    public static final int MESSAGE_TYPE = 0x00A;

    private int linkPhaseChangeMaximumCompactValue = -1;
    private int linkPhaseChangeMinimumDenseValue = -1;
    private int estimatedNumberEntries = -1;
    private int estimatedLinkNameLengthOfEntries = -1;

    public GroupInfoMessage(HeaderReader reader) {
        byte version = reader.readByte();
        Flags flags = reader.readFlags();

        if(flags.isSet(0)) {
            linkPhaseChangeMaximumCompactValue = reader.readUInt16();
            linkPhaseChangeMinimumDenseValue = reader.readUInt16();
        }

        if(flags.isSet(1)) {
            estimatedNumberEntries = reader.readUInt16();
            estimatedLinkNameLengthOfEntries = reader.readUInt16();
        }
    }
}
