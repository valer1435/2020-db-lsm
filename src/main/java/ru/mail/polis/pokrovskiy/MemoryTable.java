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
        Value oldValue = map.put(key, val);
        if (oldValue == null) {
            sizeInBytes+= key.limit()+val.getValue().limit();
        }else if (oldValue.isTombstone()){
            sizeInBytes+= val.getValue().limit();
        }else{
            sizeInBytes+= val.getValue().limit()+-oldValue.getValue().limit();
        }
    }

    public void remove(@NotNull final ByteBuffer key) throws IOException {
        Value oldValue = map.put(key,  new Value(null, true));
        if (oldValue == null){
            sizeInBytes+=key.limit();
        } else if (!oldValue.isTombstone()){
            sizeInBytes -= oldValue.getValue().limit();
        }

    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getGeneration() {
        return generation;
    }
}
