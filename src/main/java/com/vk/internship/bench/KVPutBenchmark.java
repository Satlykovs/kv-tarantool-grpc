package com.vk.internship.bench;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(8)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
public class KVPutBenchmark {

    @Benchmark
    public void putRandomKey(Blackhole bh, TarantoolState state) {
        int idx = ThreadLocalRandom.current().nextInt(TarantoolState.TOTAL_KEYS);
        String key = state.allKeys[idx];
        byte[] value = new byte[64];
        state.repository.putAsync(key, value).join();
        bh.consume(value);
    }
}