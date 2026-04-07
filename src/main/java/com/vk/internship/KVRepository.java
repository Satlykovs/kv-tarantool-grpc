package com.vk.internship;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.client.factory.TarantoolBoxClientBuilder;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.Tuple;
import io.tarantool.pool.InstanceConnectionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


public class KVRepository
{
    private static final Logger logger = LoggerFactory.getLogger(KVRepository.class);
    private final TarantoolBoxClient client;
    private final String spaceName;

    public KVRepository(String host, int port, String username, String password, String spaceName)
    {
        try
        {
            this.spaceName = spaceName;


            InstanceConnectionGroup group = InstanceConnectionGroup.builder().withHost(host)
                    .withPort(port).withUser(username)
                    .withPassword(password != null && !password.isEmpty() ? password : null)
                    .withSize(8).build();

            List<InstanceConnectionGroup> groups = Collections.singletonList(group);

            TarantoolBoxClientBuilder builder = TarantoolFactory.box().withGroups(groups);


            this.client = builder.build();

            logger.info("Successfully connected to Tarantool at {}:{}", host, port);
        } catch (Exception e)
        {
            throw new RuntimeException("Failed to connect to Tarantool", e);
        }
    }

    private TarantoolBoxSpace space()
    {
        return client.space(spaceName);
    }

    public CompletableFuture<Void> putAsync(String key, byte[] value)
    {

        return space().replace(Arrays.asList(key, value))
                .thenApply(ignored -> null);
    }

    public CompletableFuture<byte[]> getAsync(String key)
    {
        SelectOptions opts = SelectOptions.builder()
                .withIterator(BoxIterator.EQ)
                .withLimit(1)
                .build();

        return space().select(Collections.singletonList(key), opts)
                .thenApply(response ->
                {
                    List<Tuple<List<?>>> tuples = response.get();

                    if (tuples == null || tuples.isEmpty()) return null;

                    List<?> fields = tuples.getFirst().get();
                    if (fields.size() <= 1) return null;
                    return (fields.get(1) instanceof byte[] byteArray) ? byteArray :
                            null;
                });

    }

    public CompletableFuture<Void> deleteAsync(String key)
    {
        return space().delete(Collections.singletonList(key))
                .thenApply(ignored -> null);
    }

    public CompletableFuture<Void> rangeAsync(String keySince, String keyTo,
                                              int batchSize,
                                              Consumer<Map.Entry<String, byte[]>> consumer)
    {
        return rangeBatchAsync(keySince, keyTo, batchSize, null, true, consumer);
    }

    public CompletableFuture<Long> countAsync()
    {
        String script = String.format("return box.space['%s']:len()", spaceName);

        return client.eval(script)
                .thenApply(result ->
                {
                    List<?> list = result.get();
                    Object val = list.getFirst();
                    return ((Number) val).longValue();
                });

    }

    public void close()
    {
        if (client != null)
        {
            try
            {
                client.close();
            } catch (Exception e)
            {
                logger.error("Error closing Tarantool client: {}", e.getMessage());
            }
        }
    }


    private CompletableFuture<Void> rangeBatchAsync(String keySince, String keyTo,
                                                    int batchSize, String lastKey,
                                                    boolean isFirst,
                                                    Consumer<Map.Entry<String, byte[]>> consumer)
    {

        BoxIterator iterator = isFirst ? BoxIterator.GE : BoxIterator.GT;
        String searchKey = isFirst ? keySince : lastKey;

        SelectOptions opts = SelectOptions.builder()
                .withLimit(batchSize)
                .withIterator(iterator)
                .build();

        return space().select(Collections.singletonList(searchKey), opts)
                .thenCompose(response ->
                {
                    List<Tuple<List<?>>> tuples = response.get();

                    if (tuples == null || tuples.isEmpty())
                    {
                        return CompletableFuture.completedFuture(null);
                    }
                    for (Tuple<List<?>> tuple : tuples)
                    {
                        List<?> fields = tuple.get();

                        String key = (String) fields.getFirst();

                        if (key.compareTo(keyTo) > 0)
                        {
                            return CompletableFuture.completedFuture(null);
                        }

                        byte[] value =
                                (fields.size() > 1 && fields.get(
                                        1) instanceof byte[] byteArray) ? byteArray : null;

                        consumer.accept(new AbstractMap.SimpleEntry<>(key, value));
                    }

                    if (tuples.size() == batchSize)
                    {
                        String newLastKey = (String) tuples.getLast().get().getFirst();
                        return rangeBatchAsync(keySince, keyTo, batchSize, newLastKey, false,
                                consumer);
                    } else
                    {
                        return CompletableFuture.completedFuture(null);
                    }
                });


    }

}
