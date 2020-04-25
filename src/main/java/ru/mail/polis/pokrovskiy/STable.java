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
    private List<Integer> offsetList;
    private long generation;

    public STable(Path file, List<Integer> offsetList, long generation) {
        this.file = file;
        this.offsetList = offsetList;
        this.generation = generation;
    }

    public static List<STable> findTables(Path path) throws IOException {
        List<STable> tables = new ArrayList<>();
        Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path file,
                    final BasicFileAttributes attrs) throws IOException {

                if (file.getFileName().toString().endsWith(".data")) {
                    tables.add(readTableFromMemory(file));

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

            Iterator<Cell> iter = table.iterator(MyDAO.MIN_BYTE_BUFFER);
            while (iter.hasNext()) {
                Cell cell = iter.next();
                offsetList.add((int) channel.position());

                final long keySize = cell.getKey().limit();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(keySize).flip());
                channel.write(ByteBuffer.allocate((int) keySize).put(cell.getKey().flip()));

                final long timeStamp = cell.getValue().getTimestamp();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(timeStamp).flip());

                final boolean tombstone = cell.getValue().isTombstone();
                channel.write(ByteBuffer.allocate(Byte.BYTES).put((byte) (tombstone ? 1 : 0)).flip());

                if (!tombstone) {
                    final long valueSize = cell.getKey().limit();
                    channel.write(ByteBuffer.allocate(Long.BYTES).putLong(valueSize).flip());
                    channel.write(ByteBuffer.allocate((int) valueSize).put(cell.getValue().getValue().flip()));
                }
            }

            for (Integer offset : offsetList) {
                final ByteBuffer offsetByteBuffer = ByteBuffer.allocate(Long.BYTES);
                offsetByteBuffer.putLong(offset).flip();
                channel.write(offsetByteBuffer);
            }
            final ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
            rowCountBuffer.putInt(offsetList.size()).flip();
            channel.write(rowCountBuffer);

            return new STable(path, offsetList, table.getGeneration());
        }
    }

    public Cell parseCell(int offset) throws IOException {
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ)) {
            final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(keySizeBuffer, offset);

            offset += Long.BYTES;

            final long keySize = keySizeBuffer.rewind().getLong();
            final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
            channel.read(keyBuffer, offset);

            offset += (int) keySize;

            final ByteBuffer timeStampBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(timeStampBuffer, offset);
            final long timeStamp = timeStampBuffer.rewind().getLong();
            offset += Long.BYTES;

            final ByteBuffer isTombstoneBuffer = ByteBuffer.allocate(Byte.BYTES);
            channel.read(isTombstoneBuffer, offset);
            final boolean isTombstone = isTombstoneBuffer.rewind().get() == 1;
            offset += Byte.BYTES;
            Value value;
            if (!isTombstone) {
                final ByteBuffer valueSizeBuffer = ByteBuffer.allocate(Long.BYTES);
                channel.read(valueSizeBuffer, offset);
                final long valueSize = valueSizeBuffer.rewind().getLong();
                offset += Long.BYTES;
                final ByteBuffer valueBuffer = ByteBuffer.allocate((int) valueSize);
                channel.read(valueBuffer, offset);

                value = new Value(valueBuffer, timeStamp, false);
            } else {
                value = new Value(null, timeStamp, true);
            }
            return Cell.of(keyBuffer, value);
        }
    }


    public Iterator<Cell> iteratorFromTable(ByteBuffer from) throws IOException {

        Iterator<Cell> iterator = new Iterator<>() {
            int index = findIndex(from, 0, offsetList.size() - 1);

            @Override
            public boolean hasNext() {
                if (index < offsetList.size()) {
                    return true;
                }
                return false;
            }

            @Override
            public Cell next() {
                try {
                    return parseCell(index++);
                } catch (IOException e) {
                    return null;
                }
            }
        };
        return iterator;
    }

    public static STable readTableFromMemory(Path path) throws IOException {
        int gen = getVersionFromName(path.getFileName().toString());
        List<Integer> offsetList = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ)) {
            final ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
            channel.read(rowCountBuffer, channel.size() - Integer.BYTES);
            int rowCount = rowCountBuffer.rewind().getInt();
            long startPosition = channel.size() - Integer.BYTES - rowCount * Long.BYTES;
            for (int i = 0; i < rowCount; i++) {
                ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
                channel.read(offsetBuffer, startPosition);
                offsetList.add((int) offsetBuffer.flip().getLong());
                startPosition += Long.BYTES;
            }

            return new STable(path, offsetList, gen);
        }
    }

    public static int getVersionFromName(String fileName) {
        return Integer.parseInt(Iterables.get(Splitter.on("LSM-DB-GEN-").split(fileName), 1).replaceAll("\\.data", ""));
    }

    public int findIndex(ByteBuffer from, int left, int right) throws IOException {
        int curLeft = left;
        int curRight = right;

        while (curLeft <= curRight) {
            final int mid = (curLeft + curRight) / 2;

            final ByteBuffer midKey = parseKey(mid);

            final int compare = midKey.compareTo(from);


            if (compare < 0) {
                curLeft = mid + 1;
            } else if (compare > 0) {
                curRight = mid - 1;
            } else {
                return mid;
            }
        }
        return curLeft;
    }

    public ByteBuffer parseKey(int index) throws IOException {
        int offset = offsetList.get(index);
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.READ)) {
            final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(keySizeBuffer, offset);
            final long keySize = keySizeBuffer.rewind().getLong();
            offset += Long.BYTES;
            final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
            channel.read(keyBuffer, offset);
            return keyBuffer;
        }
    }

    public long getGeneration() {
        return generation;
    }

    @Override
    public int compareTo(@NotNull STable sTable) {
        return Long.compare(generation, sTable.getGeneration());
    }
}
