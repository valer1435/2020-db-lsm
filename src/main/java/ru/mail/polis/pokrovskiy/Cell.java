package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell implements Comparable<Cell> {
    private final ByteBuffer key;
    private final Value value;
    private final long generation;

    Cell(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
             final long generation) {
        this.key = key;
        this.value = value;
        this.generation = generation;
    }

    public static Cell of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            final long generation) {
        return new Cell(key, value, generation);
    }

    public @NotNull ByteBuffer getKey() {
        return key;
    }

    public @NotNull Value getValue() {
        return value;
    }

    private @NotNull Long getGeneration() {
        return generation;
    }

    @Override
    public int compareTo(@NotNull final Cell cell) {
        return Comparator
                .comparing(Cell::getKey)
                .thenComparing(Cell::getValue)
                .thenComparing(Comparator.comparingLong(Cell::getGeneration).reversed())
                .compare(this, cell);

    }
}
