package com.vk.internship.bench;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Threads(8)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
public class KVGetBenchmark
{


    @Benchmark
    public void getFixedKey(Blackhole bh, TarantoolState state)
    {
        byte[] value = state.repository.getAsync("k2500000").join();
        bh.consume(value);
    }

    @Benchmark
    public void getRandomKey(Blackhole bh, TarantoolState state)
    {
        int idx = ThreadLocalRandom.current().nextInt(TarantoolState.TOTAL_KEYS);
        String key = state.allKeys[idx];

        byte[] value = state.repository.getAsync(key).join();
        bh.consume(value);
    }


}
