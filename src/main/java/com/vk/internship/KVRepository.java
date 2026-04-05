package com.vk.internship;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.Tuple;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;


public class KVRepository
{
    private static final Logger logger = Logger.getLogger(KVRepository.class.getName());
    private final TarantoolBoxClient client;

    public KVRepository(String host, int port, String username, String password)
    {
        try
        {
            var builder = TarantoolFactory.box()
                    .withHost(host)
                    .withPort(port)
                    .withUser(username);
            if (password != null && !password.isEmpty())
            {
                builder = builder.withPassword(password);
            }
            this.client = builder.build();
        } catch (Exception e)
        {
            throw new RuntimeException("Failed to connect to Tarantool", e);
        }
    }

    private TarantoolBoxSpace space()
    {
        return client.space("KV");
    }

    public void put(String key, byte[] value) throws ExecutionException, InterruptedException
    {

        space().replace(Arrays.asList(key, value)).get();
    }

    public byte[] get(String key) throws ExecutionException, InterruptedException
    {
        SelectResponse<List<Tuple<List<?>>>> response = space().select(
                Collections.singletonList(key)).get();

        List<Tuple<List<?>>> tuples = response.get();

        if (tuples == null || tuples.isEmpty()) return null;

        List<?> fields = tuples.getFirst().get();
        if (fields.size() <= 1) return null;


        if (fields.get(1) instanceof byte[] byteArray)
        {
            return byteArray;
        }
        return null;
    }

    public void delete(String key) throws ExecutionException, InterruptedException
    {
        space().delete(Collections.singletonList(key)).get();
    }

    public Iterator<Map.Entry<String, byte[]>> range(String keySince, String keyTo, int batchSize)
    {
        return new RangeIterator(keySince, keyTo, batchSize);
    }

    public long count() throws ExecutionException, InterruptedException
    {
        List<?> result = client.eval("return box.space.KV:len()").get().get();
        return (long) result.getFirst();
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
                logger.severe("Error closing Tarantool client: " + e.getMessage());
            }
        }
    }

    private class RangeIterator implements Iterator<Map.Entry<String, byte[]>>
    {

        private final String keyTo;
        private final int batchSize;
        private final String keySince;

        private String lastKey = null;
        private List<Tuple<List<?>>> currentBatch;
        private int currentIndex = 0;
        private boolean finished = false;
        private boolean isFirstLoad = true;

        RangeIterator(String keySince, String keyTo, int batchSize)
        {
            this.keySince = keySince;
            this.keyTo = keyTo;
            this.batchSize = batchSize;

            loadBatch();
        }

        private void loadBatch()
        {
            try
            {
                BoxIterator iterator = isFirstLoad ? BoxIterator.GE : BoxIterator.GT;
                String searchKey = isFirstLoad ? keySince : lastKey;

                SelectOptions options = SelectOptions.builder()
                        .withLimit(batchSize)
                        .withIterator(iterator)
                        .build();

                SelectResponse<List<Tuple<List<?>>>> response = space().select(
                        Collections.singletonList(searchKey), options).get();

                currentBatch = response.get();

                if (currentBatch == null || currentBatch.isEmpty())
                {
                    finished = true;
                } else
                {
                    Tuple<List<?>> lastTuple = currentBatch.getLast();
                    lastKey = (String) lastTuple.get().getFirst();
                    currentIndex = 0;
                    isFirstLoad = false;
                }
            } catch (Exception e)
            {
                finished = true;
                currentBatch = null;
            }
        }

        @Override
        public boolean hasNext()
        {
            if (finished) return false;
            if (currentBatch != null && currentIndex < currentBatch.size())
            {
                String nextKey = (String) currentBatch.get(currentIndex).get().getFirst();
                if (nextKey.compareTo(keyTo) > 0)
                {
                    finished = true;
                    return false;
                }
                return true;
            }
            if (currentBatch != null && currentBatch.size() == batchSize)
            {
                loadBatch();
                return hasNext();
            }
            return false;
        }

        @Override
        public Map.Entry<String, byte[]> next()
        {
            if (!hasNext()) throw new NoSuchElementException();

            Tuple<List<?>> tuple = currentBatch.get(currentIndex++);

            List<?> fields = tuple.get();
            String key = (String) fields.getFirst();

            if (key.compareTo(keyTo) > 0)
            {
                finished = true;
                throw new NoSuchElementException();
            }

            byte[] value = null;
            if (fields.size() > 1 && fields.get(1) instanceof byte[] byteArray)
            {
                value = byteArray;
            }
            return new AbstractMap.SimpleEntry<>(key, value);

        }
    }
}
