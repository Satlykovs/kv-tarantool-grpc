package com.vk.internship;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DisplayName("Интеграционные тесты работы KVRepository")
public class KVRepositoryIT
{


    @Container
    public static TarantoolContainer tarantool = new TarantoolContainer("tarantool/tarantool:3.2")
            .withDirectoryBinding("tarantool")
            .withScriptFileName("init.lua")
            .withUsername("guest")
            .withPassword("");


    private static KVRepository repository;

    @BeforeAll
    static void setup()
    {
        repository = new KVRepository(tarantool.getHost(), tarantool.getPort(),
                tarantool.getUsername(),
                tarantool.getPassword(), "KV");
    }

    @Test
    @DisplayName("Базовые операции: сохранение, получение и удаление данных")
    void testBasicOperations()
    {
        String key = "key";
        byte[] data = "some data".getBytes();

        repository.putAsync(key, data).join();

        byte[] result = repository.getAsync(key).join();
        assertArrayEquals(data, result, "Данные в базе должны совпасть с исходными");

        repository.deleteAsync(key).join();
        assertNull(repository.getAsync(key).join(), "После удаления ключ должен возвращать null");
    }

    @Test
    @DisplayName("Обработка NULL: запись и получение null-value")
    void testNullValueHandling()
    {
        String key = "key";

        repository.putAsync(key, null).join();

        byte[] result = repository.getAsync(key).join();

        assertNull(result, "Если в базе лежит null, то get должен вернуть null");
    }

    @Test
    @DisplayName("RANGE: Проверка стриминга")
    void testRangeWithStream()
    {
        int total = 1500;
        for (int i = 1; i <= total; ++i)
        {
            String key = String.format("k%04d", i);
            repository.putAsync(key, new byte[]{(byte) (i % 256)}).join();
        }

        List<Map.Entry<String, byte[]>> entries = new ArrayList<>();
        repository.rangeAsync("k0001", "k1500", 500, entries::add).join();

        assertEquals(total, entries.size(), "Должны быть получены все записи");

        int count = 0;

        for (Map.Entry<String, byte[]> entry : entries)
        {
            count++;
            assertEquals(String.format("k%04d", count), entry.getKey());
        }

    }

    @Test
    @DisplayName("OVERWRITE: Проверка перезаписи по существующему ключу")
    void testOverwrite()
    {
        String key = "key";

        repository.putAsync(key, "first".getBytes()).join();

        repository.putAsync(key, "second".getBytes()).join();

        byte[] result = repository.getAsync(key).join();
        assertEquals("second", new String(result),
                "В базе должны быть сохранено последнее значение");
    }

    @Test
    @DisplayName("COUNT: Проверка  подсчета записей")
    void testCount()
    {
        long startCount = repository.countAsync().join();

        repository.putAsync("count_test_1", new byte[]{1}).join();
        repository.putAsync("count_test_2", new byte[]{2}).join();

        assertEquals(startCount + 2, repository.countAsync().join(),
                "Счетчик записей должен увеличиться ровно на 2");

        repository.deleteAsync("count_test_1").join();
        assertEquals(startCount + 1, repository.countAsync().join(),
                "Счетчик должен уменьшиться после удаления");
    }
}
