package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell implements Comparable<Cell> {
    private final ByteBuffer key;
    private final Value value;
    private final long generation;

    Cell(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            @NotNull final long generation) {
        this.key = key;
        this.value = value;
        this.generation = generation;
    }

    public static Cell of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            @NotNull final long generation) {
        return new Cell(key, value, generation);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    public long getGeneration() {
        return generation;
    }

    @Override
    public int compareTo(@NotNull Cell cell) {
        return Comparator
                .comparing(Cell::getKey)
                .thenComparing(Cell::getValue)
                .thenComparing(Comparator.comparingLong(Cell::getGeneration).reversed())
                .compare(this, cell);

    }
}