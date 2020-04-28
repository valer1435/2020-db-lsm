package ru.mail.polis.pokrovskiy;

import com.google.common.collect.Comparators;
import com.google.common.collect.Iterators;

import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

public class MyDAO implements DAO {
    public static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private long maxSize;
    private Path filesPath;
    private MemoryTable memTable;
    private long generation;
    private final List<STable> sTables;
    private final double ALLOW_PERCENT = 0.016;


    public MyDAO(Path filesPath, long maxSize) throws IOException {
        this.maxSize = (long) (maxSize * ALLOW_PERCENT);
        System.out.println(this.maxSize);
        this.filesPath = filesPath;
        sTables = STable.findTables(filesPath);
        if (sTables.size() != 0) {
            generation = sTables.stream().max(STable::compareTo).get().getGeneration() + 1;
        } else {
            generation = 0;
        }
        memTable = new MemoryTable(generation);

    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        ArrayList<Iterator<Cell>> cellIterator = new ArrayList<Iterator<Cell>>();
        for (STable table : sTables) {
            cellIterator.add(table.iteratorFromTable(from));
        }
        cellIterator.add(memTable.iterator(from));
        final UnmodifiableIterator<Cell> sortedIterator =
                Iterators.mergeSorted(cellIterator, Comparator.naturalOrder());

        final Iterator<Cell> collapsedIterator = Iters.collapseEquals(sortedIterator, Cell::getKey);
        final Iterator<Cell> filteredIterator = Iterators.filter(collapsedIterator, cell -> !cell.getValue().isTombstone());
        Iterator<Record> it = Iterators.transform(filteredIterator, cell -> Record.of(cell.getKey(), cell.getValue().getValue()));
//        while (it.hasNext()){
//            System.out.println("ssss");
//            if (it.next().getKey().equals(from))
//                System.out.println("match");
//        }
        return it;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());

        if (memTable.getSizeInBytes() > maxSize) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());

        if (memTable.getSizeInBytes() > maxSize) {

            flush();
        }
    }

    public void flush() throws IOException {
        sTables.add(STable.writeTable(memTable, filesPath));
        generation += 1;
        memTable = new MemoryTable(generation);
    }

    @Override
    public void close() throws IOException {
        if (memTable.getSizeInBytes() > 0){
            flush();
        }
        for (STable table: sTables
             ) {
            table.close();
        }

    }
}
