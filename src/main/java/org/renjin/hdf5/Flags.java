package org.renjin.hdf5;

public class Flags {
    private byte value;

    public Flags(byte value) {
        this.value = value;
    }

    public boolean isSet(int bitIndex) {
        return (value & (1 << bitIndex)) != 0;
    }

    public byte value() {
        return value;
    }
}
