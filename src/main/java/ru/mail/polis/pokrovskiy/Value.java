package ru.mail.polis.pokrovskiy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {
    private final long timestamp;
    private final ByteBuffer data;
    private final boolean isTombstone;

    Value(@Nullable ByteBuffer data, final boolean isTombstone) {
        this.data = data;
        this.timestamp = System.currentTimeMillis();
        this.isTombstone = isTombstone;
    }

    Value(@Nullable final ByteBuffer data, final long timestamp, final boolean isTombstone) {
        this.data = data;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
    }

    long getTimestamp() {
        return timestamp;
    }

    @Nullable
    ByteBuffer getData() {
        return data;
    }

    boolean isTombstone() {
        return isTombstone;
    }

    @Override
    public int compareTo(final Value o) {
        return Long.compare(o.getTimestamp(), this.getTimestamp());
    }
}
