package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Value implements Comparable<Value>{
    private long timestamp;
    private ByteBuffer value;
    private boolean isTombstone;

    public Value(ByteBuffer value, boolean isTombstone) {
        this.value = value;

        timestamp = System.currentTimeMillis();
        this.isTombstone = isTombstone;
    }

    public Value(ByteBuffer value, long timestamp, boolean isTombstone) {
        this.value = value;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public boolean isTombstone() {
        return isTombstone;
    }
    @Override
    public int compareTo(final Value o) {
        return Long.compare(o.getTimestamp(), this.getTimestamp());
    }
}