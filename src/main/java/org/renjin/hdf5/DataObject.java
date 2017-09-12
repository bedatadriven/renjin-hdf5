package org.renjin.hdf5;


import org.renjin.hdf5.message.*;
import org.renjin.repackaged.guava.collect.Iterables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class DataObject {

    private static final int MESSAGE_SHARED_BIT = 1;

    private final List<Message> messages = new ArrayList<>();
    private Superblock superblock;


    public DataObject(FileChannel channel, Superblock superblock, long address) throws IOException {
        this.superblock = superblock;

        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, address, channel.size() - address);
        HeaderReader reader = new HeaderReader(superblock, mappedByteBuffer);

        if(reader.peekByte() == 'O') {
            readVersion2(reader);
        } else {
            readVersion1(reader);
        }
    }

    private void readVersion1(HeaderReader reader) throws IOException {
        byte version = reader.readByte();
        if(version != 1) {
            throw new IOException("Unsupported data object header version: " + version);
        }
        byte reserved0 = reader.readByte();
        int totalNumberOfMessages = reader.readUInt16();
        long objectReferenceCount = reader.readUInt32();
        long objectHeaderSize = reader.readUInt32();

        for (int i = 0; i < totalNumberOfMessages; i++) {
            int messageType = reader.readUInt16();
            System.out.println("Message Type: " + Integer.toHexString(messageType));
            int messageDataSize = reader.readUInt16();

            if(messageType != 0) {
                Flags messageFlags = reader.readFlags();
                byte[] padding = reader.readBytes(3);
                byte[] messageData = reader.readBytes(messageDataSize);

                if (messageFlags.isSet(MESSAGE_SHARED_BIT)) {
                    throw new UnsupportedOperationException("Shared message");
                } else {
                    messages.add(createMessage(messageType, messageData));
                }
            }
        }
    }

    private void readVersion2(HeaderReader reader) throws IOException {
        reader.checkSignature("OHDR");

        byte version = reader.readByte();
        if(version != 2) {
            throw new IOException("Unsupported data object header version: " + version);
        }
        Flags flags = reader.readFlags();

        if (flags.isSet(5)) {
            int accessTime = reader.getInt();
            int modificationTime = reader.getInt();
            int changeTime = reader.getInt();
            int birthTime = reader.getInt();
        }

        if (flags.isSet(4)) {
            int maxNumberCompactAttributes = reader.readUInt16();
            int maxNumberDenseAttributes = reader.readUInt16();
        }

        int chunkLength = reader.readVariableLengthSizeAsInt(flags);

        reader.updateLimit(chunkLength);

        while(reader.remaining() > 0) {
            int messageType = reader.readUInt8();
            int messageDataSize = reader.readUInt16();
            byte messageFlags = reader.readByte();

            short messageCreationOrder;
            if (flags.isSet(2)) {
                messageCreationOrder = reader.readByte();
            }
            byte[] messageData = reader.readBytes(messageDataSize);

            System.out.println(Integer.toHexString(messageType));

            messages.add(createMessage(messageType, messageData));
        }
    }

    private Message createMessage(int messageType, byte[] messageData) throws IOException {

        HeaderReader reader = new HeaderReader(superblock, ByteBuffer.wrap(messageData));
        switch (messageType) {
            case LinkInfoMessage.TYPE:
                return new LinkInfoMessage(reader);
            case LinkMessage.TYPE:
                return new LinkMessage(reader);
            case GroupInfoMessage.TYPE:
                return new GroupInfoMessage(reader);
            case DataspaceMessage.TYPE:
                return new DataspaceMessage(reader);
            case DatatypeMessage.TYPE:
                return new DatatypeMessage(reader);
            case FillValueMessage.TYPE:
                return new FillValueMessage(reader);
            case DataLayoutMessage.TYPE:
                return new DataLayoutMessage(reader);
            case DataStorageMessage.TYPE:
                return new DataStorageMessage(reader);
            default:
                return new UnknownMessage(messageType, messageData);
        }
    }

    public <T extends Message> Iterable<T> getMessages(Class<T> messageClass) {
        return Iterables.filter(messages, messageClass);
    }

    public <T extends Message> T getMessage(Class<T> messageClass) {
        return Iterables.getOnlyElement(getMessages(messageClass));
    }
}