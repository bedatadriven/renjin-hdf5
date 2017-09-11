package org.renjin.hdf5.message;

public class UnknownMessage extends Message {
    private int type;
    private final byte[] messageData;

    public UnknownMessage(int messageType, byte[] messageData) {
        this.type = messageType;
        this.messageData = messageData;
    }
}
