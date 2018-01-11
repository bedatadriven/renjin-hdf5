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

    private final Hdf5Data file;
    private final List<Message> messages = new ArrayList<>();

    public DataObject(Hdf5Data file, long address) throws IOException {
        this.file = file;

        HeaderReader reader = file.readerAt(address);
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

        int messagesRead = 0;
        while(messagesRead < totalNumberOfMessages) {
            int messageType = reader.readUInt16();
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
                messagesRead ++;
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
            int accessTime = reader.readInt();
            int modificationTime = reader.readInt();
            int changeTime = reader.readInt();
            int birthTime = reader.readInt();
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

            messages.add(createMessage(messageType, messageData));
        }
    }

    private Message createMessage(int messageType, byte[] messageData) throws IOException {

        HeaderReader reader = new HeaderReader(file.getSuperblock(), ByteBuffer.wrap(messageData));
        switch (messageType) {
            case LinkInfoMessage.MESSAGE_TYPE:
                return new LinkInfoMessage(reader);
            case LinkMessage.MESSAGE_TYPE:
                return new LinkMessage(reader);
            case GroupInfoMessage.MESSAGE_TYPE:
                return new GroupInfoMessage(reader);
            case DataspaceMessage.MESSAGE_TYPE:
                return new DataspaceMessage(reader);
            case DatatypeMessage.MESSAGE_TYPE:
                return new DatatypeMessage(reader);
            case FillValueMessage.MESSAGE_TYPE:
                return new FillValueMessage(reader);
            case DataLayoutMessage.MESSAGE_TYPE:
                return new DataLayoutMessage(reader);
            case DataStorageMessage.MESSAGE_TYPE:
                return new DataStorageMessage(reader);
            case SymbolTableMessage.MESSAGE_TYPE:
                return new SymbolTableMessage(reader);
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

    public boolean hasMessage(Class<? extends Message> messageClass) {
        for (Message message : messages) {
            if(message.getClass().equals(messageClass)) {
                return true;
            }
        }
        return false;
    }
}