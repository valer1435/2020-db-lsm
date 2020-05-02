package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

class MemoryTable {
    private final SortedMap<ByteBuffer, Value> map;
    private long sizeInBytes;
    private final long generation;

    MemoryTable(final long generation) {
        map = new TreeMap<>();
        this.generation = generation;
    }

    Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return map.tailMap(from).entrySet()
                .stream()
                .map(o -> Cell.of(o.getKey(), o.getValue(), getGeneration())).iterator();
    }

    void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {

        final Value val = new Value(value, false);
        final Value oldValue = map.put(key, val);
        if (oldValue == null) {
            sizeInBytes += key.limit() + val.getData().limit();
        } else if (oldValue.isTombstone()) {
            sizeInBytes += val.getData().limit();
        } else {
            sizeInBytes += val.getData().limit() + -oldValue.getData().limit();
        }
    }

    void remove(@NotNull final ByteBuffer key) {
        final Value oldValue = map.put(key, new Value(null, true));
        if (oldValue == null) {
            sizeInBytes += key.limit();
        } else if (!oldValue.isTombstone()) {
            sizeInBytes -= oldValue.getData().limit();
        }

    }

    long getSizeInBytes() {
        return sizeInBytes;
    }

    long getGeneration() {
        return generation;
    }
}
