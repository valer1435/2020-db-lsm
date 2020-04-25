package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;

public class Cell implements Comparable<Cell> {
    private final ByteBuffer key;
    private final Value value;

    Cell(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public static Cell of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        return new Cell(key, value);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull Cell cell) {
        return -Long.compare(value.getTimestamp(), cell.getValue().getTimestamp());
    }
}
