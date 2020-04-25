package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable {
    private final SortedMap<ByteBuffer, Value> map;
    private long sizeInBytes;
    private final long generation;


    public MemoryTable(long generation) {
        map = new TreeMap<>();
        this.generation = generation;
    }

    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return map.tailMap(from).entrySet().stream().map(o -> Cell.of(o.getKey(), o.getValue(), getGeneration())).iterator();
    }

    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {

        Value val = new Value(value, false);
        Value oldValue = map.getOrDefault(key, null);
        if (oldValue != null) {
            if (!oldValue.isTombstone()) {
                sizeInBytes -= oldValue.getValue().limit();
            }
        } else {
            sizeInBytes += key.limit();
        }
        sizeInBytes += val.getValue().limit();
        map.put(key, val);
    }

    public void remove(@NotNull final ByteBuffer key) throws IOException {
        Value val = new Value(null, true);
        Value oldValue = map.getOrDefault(key, null);
        if (oldValue != null) {
            if (!oldValue.isTombstone()) {
                sizeInBytes -= oldValue.getValue().limit();
            }
        } else {
            sizeInBytes += key.limit();
        }
        map.put(key, val);
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getGeneration() {
        return generation;
    }
}
