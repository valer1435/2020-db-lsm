package ru.mail.polis.pokrovskiy;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MyDAO implements DAO {
    static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private final long maxSize;
    private final Path filesPath;
    private final MemoryTable memTable;
    private long generation;
    private final List<STable> tableList;
    private static final double PERCENT = 0.016;

    /** Имплементация Key-value хранилища.
     * @param filesPath - путь до файла
     * @param maxSize - максимальный размер
     * @throws IOException - сли возникли ошибки с файлами
     */
    public MyDAO(@NotNull final Path filesPath, final long maxSize) throws IOException {
        this.maxSize = (long) (maxSize * PERCENT);
        this.filesPath = filesPath;
        this.tableList = STable.findTables(filesPath);
        this.generation = tableList.size() + 1L;
        this.memTable = new MemoryTable(generation);

    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> cellIterator = new ArrayList<>();
        for (final STable table : tableList) {
            cellIterator.add(table.iteratorFromTable(from));
        }
        cellIterator.add(memTable.iterator(from));
        final UnmodifiableIterator<Cell> sortedIterator =
                Iterators.mergeSorted(cellIterator, Comparator.naturalOrder());
        final Iterator<Cell> collapsedIterator = Iters.collapseEquals(sortedIterator, Cell::getKey);
        final Iterator<Cell> filteredIterator = Iterators
                .filter(collapsedIterator, cell -> !cell.getValue().isTombstone());
        return Iterators.transform(filteredIterator, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
        flushIfNeed();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
        flushIfNeed();
    }

    private void flushIfNeed() throws IOException {
        if (memTable.getSizeInBytes() > maxSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        tableList.add(STable.writeTable(memTable, filesPath));
        generation += 1;
        memTable.restart();
        generation = memTable.getGeneration();
    }

    @Override
    public void close() throws IOException {
        if (memTable.getSizeInBytes() > 0) {
            flush();
        }
        for (final STable table : tableList) {
            table.close();
        }
    }
}
