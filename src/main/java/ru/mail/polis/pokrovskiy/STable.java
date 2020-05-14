package ru.mail.polis.pokrovskiy;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.jetbrains.annotations.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

public final class STable implements Comparable<STable> {
    private static final String EXTENSION = ".data";
    private static final String PREFIX = "LSM-DB-GEN-";
    private final long generation;
    private final int rowCount;
    private final FileChannel channel;
    private final Path file;

    private STable(@NotNull final Path file, @NotNull final Long generation) throws IOException {
        this.channel = FileChannel.open(file, StandardOpenOption.READ);
        this.generation = generation;
        this.rowCount = getRowCount();
        this.file = file;
    }

    @NotNull
    static List<STable> findTables(@NotNull final Path path) throws IOException {
        final List<STable> tables = new ArrayList<>();
        Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path file,
                    final BasicFileAttributes attrs) throws IOException {

                if (file.getFileName().toString().endsWith(EXTENSION)) {
                    tables.add(new STable(file, getVersionFromName(file.getFileName().toString())));
                }
                return FileVisitResult.CONTINUE;
            }

        });
        return tables;
    }

    static void compact(@NotNull final Path path, @NotNull Path newFilePath) throws IOException {
        Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path file,
                    final BasicFileAttributes attrs) throws IOException {

                if (file.getFileName().toString().endsWith(EXTENSION) && !file.equals(newFilePath)) {
                    Files.delete(file);
                } else {
                Files.move(newFilePath, newFilePath.resolveSibling(PREFIX + MyDAO.MIN_VERSION + EXTENSION), StandardCopyOption.ATOMIC_MOVE);
                }
                return FileVisitResult.CONTINUE;
            }

        });
    }

    @NotNull
    static STable writeTable(@NotNull final Iterator<Cell> cellIterator, @NotNull final Long generation, @NotNull final Path pathToFile) throws IOException {
        final Path path = pathToFile.resolve(PREFIX + generation + EXTENSION);
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            final List<Integer> offsetList = new ArrayList<>();
            while (cellIterator.hasNext()) {
                final Cell cell = cellIterator.next();
                offsetList.add((int) channel.position());

                final long keySize = cell.getKey().limit();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(keySize).flip());
                channel.write(ByteBuffer.allocate((int) keySize).put(cell.getKey()).flip());

                final long timeStamp = cell.getValue().getTimestamp();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(timeStamp).flip());

                final boolean tombstone = cell.getValue().isTombstone();
                channel.write(ByteBuffer.allocate(Byte.BYTES).put((byte) (tombstone ? 1 : 0)).flip());

                if (!tombstone) {
                    final long valueSize = cell.getValue().getData().limit();
                    channel.write(ByteBuffer.allocate(Long.BYTES).putLong(valueSize).flip());
                    channel.write(ByteBuffer.allocate((int) valueSize).put(cell.getValue().getData()).flip());
                }
            }

            final ByteBuffer offsetByteBuffer = ByteBuffer.allocate(Long.BYTES * offsetList.size());

            for (final int offset : offsetList) {
                offsetByteBuffer.putLong(offset);
            }

            channel.write(offsetByteBuffer.flip());
            final ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
            rowCountBuffer.putInt(offsetList.size()).flip();
            channel.write(rowCountBuffer);

            return new STable(path, generation);
        }
    }


    @NotNull
    private ByteBuffer getKey(final int index) throws IOException {
        long offset = getOffset(index);

        final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(keySizeBuffer, offset);
        final long keySize = keySizeBuffer.rewind().getLong();

        offset += Long.BYTES;

        final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
        channel.read(keyBuffer, offset);
        return keyBuffer.rewind();
    }

    @NotNull
    private Cell getCell(final int index) throws IOException {
        long offset = getOffset(index);

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

    @NotNull
    Iterator<Cell> iteratorFromTable(final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            private int position = findIndex(from, 0, rowCount - 1);

            @Override
            public boolean hasNext() {
                return position < rowCount;
            }

            @Override
            public Cell next() {
                try {
                    return getCell(position++);
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }

    private int getRowCount() throws IOException {
        final ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
        final long rowCountOff = channel.size() - Integer.BYTES;
        channel.read(rowCountBuffer, rowCountOff);
        return rowCountBuffer.rewind().getInt();
    }

    private long getOffset(final int index) throws IOException {
        final ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        final long offsetOff = channel.size() - Integer.BYTES - Long.BYTES * (long) (rowCount - index);
        channel.read(offsetBuffer, offsetOff);
        return offsetBuffer.rewind().getLong();
    }

    private static long getVersionFromName(final String fileName) {
        return Long.parseLong(Iterables.get(Splitter.on("LSM-DB-GEN-").split(fileName), 1).replaceAll("\\.data", ""));
    }

    private int findIndex(final ByteBuffer from, final int left, final int right) throws IOException {
        int curLeft = left;
        int curRight = right;

        while (curLeft <= curRight) {
            final int mid = (curLeft + curRight) / 2;

            final ByteBuffer midKey = getKey(mid);

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

    private long getGeneration() {
        return generation;
    }

    @Override
    public int compareTo(@NotNull final STable table) {
        return Long.compare(generation, table.getGeneration());
    }

    void close() throws IOException {
        channel.close();
    }


    public Path getFile() {
        return file;
    }

}
