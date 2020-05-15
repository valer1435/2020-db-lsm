package ru.mail.polis.pokrovskiy;

import com.google.common.collect.Iterators;
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
    private static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private final long maxSize;
    private final Path filesPath;
    private MemoryTable memTable;
    private long generation;
    private final List<STable> tableList;

    /** Имплементация Key-value хранилища.
     * @param filesPath - путь до файла
     * @param maxSize - максимальный размер
     * @throws IOException - сли возникли ошибки с файлами
     */
    public MyDAO(@NotNull final Path filesPath, final long maxSize) throws IOException {
        this.maxSize = maxSize;
        this.filesPath = filesPath;
        this.tableList = STable.findTables(filesPath);
        this.generation = tableList.size() + 1L;
        this.memTable = new MemoryTable(generation);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> cellIterator = cellIterator(from);
        final Iterator<Cell> filteredIterator = Iterators
                .filter(cellIterator, cell -> !cell.getValue().isTombstone());
        return Iterators.transform(filteredIterator, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> cellIterator = new ArrayList<>();
        for (final STable table : tableList) {
            cellIterator.add(table.iteratorFromTable(from));
        }
        cellIterator.add(memTable.iterator(from));
        final Iterator<Cell> sortedIterator =
                Iterators.mergeSorted(cellIterator, Comparator.naturalOrder());
        return Iters.collapseEquals(sortedIterator, Cell::getKey);
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
        tableList.add(STable.writeTable(memTable.iterator(MIN_BYTE_BUFFER), memTable.getGeneration(), filesPath));
        generation += 1;
        memTable = new MemoryTable(generation);
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

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> cells = cellIterator(MIN_BYTE_BUFFER);
        final STable compactTable = STable.writeTable(cells, generation, filesPath);
        for (final STable table: tableList) {
            table.close();
        }
        tableList.clear();
        STable.deleteOldTables(filesPath, compactTable.getFile());
        tableList.add(compactTable);
        generation++;
        memTable = new MemoryTable(generation);
    }
}
