package ru.mail.polis.pokrovskiy;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class MyDAO implements DAO {
    private SortedMap<ByteBuffer, ByteBuffer> map;

    public MyDAO(){
        map = new TreeMap<>();
    }
    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        // почему-то с комментариями проходит все тесты, без них не проходит один тест
//        if (!map.containsKey(from)){
//            throw new NoSuchElementException("No such key");
//        }

        return map.tailMap(from).entrySet().stream().map(o -> Record.of(o.getKey(), o.getValue())).iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        if (!map.containsKey(key)){
            throw new IOException("No such key");
        }
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
