package org.renjin.hdf5;


import org.renjin.hdf5.message.*;
import org.renjin.repackaged.guava.base.Optional;
import org.renjin.repackaged.guava.collect.Iterables;
import org.renjin.repackaged.guava.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class DataObject {

    private static final int MESSAGE_SHARED_BIT = 1;

    private final Hdf5Data file;
    private long address;
    private byte version;
    private final List<Message> messages = new ArrayList<>();
    private final Queue<ContinuationMessage> continuations = new ArrayDeque<>();

    public DataObject(Hdf5Data file, long address) throws IOException {
        this.file = file;
        this.address = address;

        HeaderReader reader = file.readerAt(address);
        if(reader.peekByte() == 'O') {
            readVersion2(reader);
        } else {
            readVersion1(reader);
        }
    }


    private void readVersion1(HeaderReader reader) throws IOException {
        version = reader.readByte();
        if(version != 1) {
            throw new IOException("Unsupported data object header version: " + version);
        }
        byte reserved0 = reader.readByte();
        int totalNumberOfMessages = reader.readUInt16();
        long objectReferenceCount = reader.readUInt32();
        int objectHeaderSize = reader.readUInt32AsInt();

        readMessagesVersion1(reader, objectHeaderSize);


        ContinuationMessage continuation;
        while((continuation = continuations.poll()) != null) {
            readContinuationV1(continuation);
        }
    }

    private void readContinuationV1(ContinuationMessage continuationMessage) throws IOException {
        HeaderReader reader = file.readerAt(continuationMessage.getOffset(), continuationMessage.getLength());

        // Continuation blocks for version 1 object headers have no special formatting information;
        // they are merely a list of object header message info sequences (type, size, flags, reserved bytes and
        // data for each message sequence). See the description of Version 1 Data Object Header Prefix.
        readMessagesVersion1(reader, Ints.checkedCast(continuationMessage.getLength()));
    }


    private void readMessagesVersion1(HeaderReader reader, int objectHeaderSize) throws IOException {

        while(objectHeaderSize > 0) {

            reader.alignTo(8);

            int messageType = reader.readUInt16();
            int messageDataSize = reader.readUInt16();

            if(messageType != 0) {
                Flags messageFlags = reader.readFlags();
                byte[] padding = reader.readBytes(3);
                byte[] messageData = reader.readBytes(messageDataSize);

                if (messageFlags.isSet(MESSAGE_SHARED_BIT)) {
                    throw new UnsupportedOperationException("Shared message");
                } else {
                    addMessage(createMessage(messageType, messageData));
                }
            }

            objectHeaderSize -= 8;
            objectHeaderSize -= messageDataSize;
        }
    }

    private void addMessage(Message message) {
        messages.add(message);
        if(message instanceof ContinuationMessage) {
            continuations.add((ContinuationMessage) message);
        }
    }

    private void readVersion2(HeaderReader reader) throws IOException {
        reader.checkSignature("OHDR");

        version = reader.readByte();
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

        readMessagesV2(reader, flags);

        ContinuationMessage continuation;
        while((continuation = continuations.poll()) != null) {
            readContinuationV2(continuation, flags);
        }
    }


    private void readContinuationV2(ContinuationMessage continuationMessage, Flags flags) throws IOException {
        HeaderReader reader = file.readerAt(continuationMessage.getOffset(), continuationMessage.getLength());
        reader.checkSignature("OCHK");
        readMessagesV2(reader, flags);
    }

    private void readMessagesV2(HeaderReader reader, Flags flags) throws IOException {

        // A gap in an object header chunk is inferred by the end of the messages for the chunk before the beginning
        // of the chunk’s checksum. Gaps are always smaller than the size of an object header message prefix
        // (message type + message size + message flags).
        int messageDataPrefixSize = 4;

        while(reader.remaining() > messageDataPrefixSize) {
            int messageType = reader.readUInt8();
            if(messageType == 0) {
                break;
            }

            int messageDataSize = reader.readUInt16();
            Flags messageFlags = reader.readFlags();

            short messageCreationOrder;
            if (flags.isSet(2)) {
                messageCreationOrder = reader.readByte();
            }
            byte[] messageData = reader.readBytes(messageDataSize);

            addMessage(createMessage(messageType, messageData));
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
            case ContinuationMessage.MESSAGE_TYPE:
                return new ContinuationMessage(reader);
            case DataStorageMessage.MESSAGE_TYPE:
                return new DataStorageMessage(reader);
            case AttributeMessage.MESSAGE_TYPE:
                return new AttributeMessage(reader);
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

    public <T extends Message> Optional<T> getMessageIfPresent(Class<T> messageClass) {
        for (Message message : messages) {
            if(message.getClass().equals(messageClass)) {
                return Optional.<T>of((T) message);
            }
        }
        return Optional.absent();
    }
}