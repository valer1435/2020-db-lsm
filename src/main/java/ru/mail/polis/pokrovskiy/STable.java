package ru.mail.polis.pokrovskiy;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class STable implements Comparable<STable> {
    private static final String EXTENTION = ".data";
    private static final String PREFIX = "LSM-DB-GEN-";

    private final Path file;
    private long generation;
    private int rowCount;

    public STable(Path file,  long generation) throws IOException {
        this.file = file;
        this.generation = generation;
        rowCount = receiveRowCount();
    }

    public static List<STable> findTables(Path path) throws IOException {
        List<STable> tables = new ArrayList<>();
        Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path file,
                    final BasicFileAttributes attrs) throws IOException {

                if (file.getFileName().toString().endsWith(".data")) {
                    tables.add(new STable(file, getVersionFromName(file.getFileName().toString())));

                }
                return FileVisitResult.CONTINUE;
            }

        });
        return tables;
    }

    public static STable writeTable(MemoryTable table, Path pathToFile) throws IOException {
        Path path = pathToFile.resolve(PREFIX + table.getGeneration() + EXTENTION);
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            final List<Integer> offsetList = new ArrayList<>();
            Iterator<Cell> it = table.iterator(MyDAO.MIN_BYTE_BUFFER);
            while (it.hasNext()) {

                offsetList.add((int) channel.position());

                final Cell cell = it.next();

                final long keySize = cell.getKey().limit();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(keySize).flip());
                channel.write(ByteBuffer.allocate((int) keySize).put(cell.getKey()).flip());

                final long timeStamp = cell.getValue().getTimestamp();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(timeStamp).flip());

                final boolean tombstone = cell.getValue().isTombstone();
                channel.write(ByteBuffer.allocate(Byte.BYTES).put((byte) (tombstone ? 1 : 0)).flip());

                if (!tombstone) {
                    final ByteBuffer value = cell.getValue().getValue();
                    final long valueSize = value.limit();
                    channel.write(ByteBuffer.allocate(Long.BYTES).putLong(valueSize).flip());
                    channel.write(ByteBuffer.allocate((int) valueSize).put(value).flip());
                }
            }

            final ByteBuffer offsetByteBuffer = ByteBuffer.allocate(Long.BYTES * offsetList.size());

            for (final int offset : offsetList) {
                offsetByteBuffer.putLong(offset);
            }

            channel.write(offsetByteBuffer.flip());

            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(offsetList.size()).flip());
            return new STable(path, table.getGeneration());
        }
    }

    protected ByteBuffer parseKey(final int index) throws IOException {
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ);
        long offset = receiveOffset(index);

        final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(keySizeBuffer, offset);
        final long keySize = keySizeBuffer.rewind().getLong();

        offset += Long.BYTES;

        final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
        channel.read(keyBuffer, offset);

        return keyBuffer.rewind();
    }

    protected Cell parseCell(final int index) throws IOException {
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ);
        long offset = receiveOffset(index);

        final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(keySizeBuffer, offset);
        final long keySize = keySizeBuffer.rewind().getLong();

        offset += Long.BYTES;

        final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
        channel.read(keyBuffer, offset);
        final ByteBuffer key = keyBuffer.rewind();

        offset += keySize;

        final ByteBuffer timeStampBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(timeStampBuffer, offset);
        final long timeStamp = timeStampBuffer.rewind().getLong();

        offset += Long.BYTES;

        final ByteBuffer tombstoneBuffer = ByteBuffer.allocate(Byte.BYTES);
        channel.read(tombstoneBuffer, offset);
        final boolean tombstone = tombstoneBuffer.rewind().get() != 0;

        if (tombstone) {
            return new Cell(key, new Value(null, timeStamp, true), getGeneration());
        } else {

            offset += Byte.BYTES;

            final ByteBuffer valueSizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(valueSizeBuffer, offset);
            final long valueSize = valueSizeBuffer.rewind().getLong();

            offset += Long.BYTES;

            final ByteBuffer valueBuffer = ByteBuffer.allocate((int) valueSize);
            channel.read(valueBuffer, offset);
            final ByteBuffer value = valueBuffer.rewind();

            return new Cell(key, new Value(value, timeStamp, false), getGeneration());
        }
    }


    public Iterator<Cell> iteratorFromTable(ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private int position = findIndex(from, 0, rowCount - 1);

            @Override
            public boolean hasNext() {
                return position < rowCount;
            }

            @Override
            public Cell next() {
                try {
                    return parseCell(position++);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        };
    }
    private int receiveRowCount() throws IOException {
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ);
        final ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
        final long rowCountOff = channel.size() - Integer.BYTES;
        channel.read(rowCountBuffer, rowCountOff);
        return rowCountBuffer.rewind().getInt();
    }

    private long receiveOffset(final int index) throws IOException {
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ);
        final ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        final long offsetOff = channel.size() - Integer.BYTES - Long.BYTES * (long) (rowCount - index);
        channel.read(offsetBuffer, offsetOff);
        return offsetBuffer.rewind().getLong();
    }

    public static int getVersionFromName(String fileName) {
        return Integer.parseInt(Iterables.get(Splitter.on("LSM-DB-GEN-").split(fileName), 1).replaceAll("\\.data", ""));
    }

    public int findIndex(ByteBuffer from, int low, int high) throws IOException {
        int curLow = low;
        int curHigh = high;

        while (curLow <= curHigh) {
            final int mid = (curLow + curHigh) / 2;

            final ByteBuffer midKey = parseKey(mid);

            final int compare = midKey.compareTo(from);


            if (compare < 0) {
                curLow = mid + 1;
            } else if (compare > 0) {
                curHigh = mid - 1;
            } else {
                return mid;
            }
        }
        System.out.println(curLow);
        return curLow;
    }


    public long getGeneration() {
        return generation;
    }

    @Override
    public int compareTo(@NotNull STable sTable) {
        return Long.compare(generation, sTable.getGeneration());
    }
}
