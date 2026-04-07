package com.vk.internship.bench;


import com.vk.internship.KVRepository;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


@State(Scope.Benchmark)
public class TarantoolState
{

    protected static final int BATCH_SIZE = 20_000;
    protected static final int TOTAL_KEYS = 5_000_000;

    public static final String KEY_FORMAT = "k%07d";

    protected final Logger logger = LoggerFactory.getLogger(TarantoolState.class);
    protected KVRepository repository;

    protected String[] allKeys;

    public static void main(String[] args) throws Exception
    {
        Main.main(args);
    }

    @Setup(Level.Trial)
    public void setup()
    {
        logger.info("=== Инициализация окружения для бенчмарка ===");

        String host = "localhost";
        int port = 3301;
        String username = "guest";
        String password = null;

        repository = new KVRepository(host, port, username, password, "KV");


        long count = 0;
        try
        {
            count = repository.countAsync().join();
        } catch (Exception _)
        {
            logger.warn("Не удалось получить count");
        }


        if (count >= TOTAL_KEYS)
        {
            logger.info("База данных уже содержит {} записей, пропускаем заполнение", count);
        } else
        {
            logger.info("=== Заполнение данными {} записей ===", TOTAL_KEYS);
            for (int i = 1; i <= TOTAL_KEYS; i += BATCH_SIZE)
            {
                List<CompletableFuture<Void>> batch = new ArrayList<>(BATCH_SIZE);
                int end = Math.min(i + BATCH_SIZE - 1, TOTAL_KEYS);
                for (int j = i; j <= end; j++)
                {
                    String key = String.format(KEY_FORMAT, j);
                    batch.add(repository.putAsync(key, new byte[64]));
                }
                CompletableFuture.allOf(batch.toArray(new CompletableFuture[0])).join();
                logger.info("Прогресс: {} / {}", end, TOTAL_KEYS);
            }
        }

        allKeys = new String[TOTAL_KEYS];
        for (int i = 0; i < TOTAL_KEYS; i++)
        {
            allKeys[i] = String.format(KEY_FORMAT, i + 1);
        }
        logger.info("=== База данных готова ===");

    }


    @TearDown(Level.Trial)
    public void tearDown()
    {
        logger.info("=== Завершение работы ===");

        if (repository != null) repository.close();
    }
}
