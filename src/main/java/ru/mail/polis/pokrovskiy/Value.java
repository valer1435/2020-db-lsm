package ru.mail.polis.pokrovskiy;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {
    private long timestamp;
    private ByteBuffer value;
    private boolean isTombstone;

    Value(ByteBuffer value, boolean isTombstone) {
        this.value = value;

        timestamp = System.currentTimeMillis();
        this.isTombstone = isTombstone;
    }

    Value(ByteBuffer value, long timestamp, boolean isTombstone) {
        this.value = value;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
    }

    long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getValue() {
        return value;
    }

    boolean isTombstone() {
        return isTombstone;
    }

    @Override
    public int compareTo(final Value o) {
        return Long.compare(o.getTimestamp(), this.getTimestamp());
    }
}
