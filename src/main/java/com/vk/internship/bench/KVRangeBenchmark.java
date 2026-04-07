package com.vk.internship.bench;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(8)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
public class KVRangeBenchmark
{

    @Param({"500", "1000", "2000", "5000"})
    public int rangeBatchSize;

    @Benchmark
    public void rangeFixedWindow(Blackhole bh, TarantoolState state) {

        List<Map.Entry<String, byte[]>> entries = new ArrayList<>();

        String keySince = "k1000000";
        String keyTo = String.format(TarantoolState.KEY_FORMAT, 1_000_000 + rangeBatchSize);

        state.repository.rangeAsync(keySince, keyTo, rangeBatchSize, entries::add).join();

        for (Map.Entry<String, byte[]> entry : entries) {
            bh.consume(entry);
        }
    }

    @Benchmark
    public void rangeRandomWindow(Blackhole bh, TarantoolState state) {

        int maxStart = TarantoolState.TOTAL_KEYS - rangeBatchSize;
        int start = ThreadLocalRandom.current().nextInt(maxStart) + 1;

        String keySince = String.format(TarantoolState.KEY_FORMAT, start);
        String keyTo = String.format(TarantoolState.KEY_FORMAT, start + rangeBatchSize);

        List<Map.Entry<String, byte[]>> entries = new ArrayList<>();

        state.repository.rangeAsync(keySince, keyTo, rangeBatchSize, entries::add).join();

        for (Map.Entry<String, byte[]> entry : entries) {
            bh.consume(entry);
        }
    }

    @Benchmark
    public void rangeMultiBatch(Blackhole bh, TarantoolState state) {

        int totalSize = rangeBatchSize * 5;
        int maxStart = TarantoolState.TOTAL_KEYS - totalSize;
        int start = ThreadLocalRandom.current().nextInt(maxStart) + 1;

        String keySince = String.format(TarantoolState.KEY_FORMAT, start);
        String keyTo = String.format(TarantoolState.KEY_FORMAT, start + totalSize);

        List<Map.Entry<String, byte[]>> entries = new ArrayList<>();

        state.repository.rangeAsync(keySince, keyTo, rangeBatchSize, entries::add).join();
        for (Map.Entry<String, byte[]> entry : entries) {
            bh.consume(entry);
        }
    }
}