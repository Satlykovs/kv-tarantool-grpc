package com.vk.internship;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.Map;


public class KVServiceImpl extends com.vk.internship.KVServiceGrpc.KVServiceImplBase
{
    private final KVRepository repository;

    public KVServiceImpl(String host, int port, String username, String password)
    {
        this.repository = new KVRepository(host, port, username, password);
    }

    private boolean isInvalidKey(String key)
    {
        return key == null || key.trim().isEmpty();
    }

    @Override
    public void put(com.vk.internship.PutRequest request,
                    StreamObserver<com.vk.internship.PutResponse> responseObserver)
    {
        try
        {
            String key = request.getKey();
            if (isInvalidKey(key))
            {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT.withDescription("Key cannot be null or empty")
                                .asRuntimeException());
                return;
            }
            byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;

            repository.put(key, value);

            responseObserver.onNext(
                    com.vk.internship.PutResponse.newBuilder().setSuccess(true).build());
        } catch (Exception e)
        {
            responseObserver.onNext(
                    com.vk.internship.PutResponse.newBuilder().setSuccess(false).build());
        } finally
        {
            responseObserver.onCompleted();
            ;
        }

    }

    @Override
    public void get(com.vk.internship.GetRequest request,
                    StreamObserver<com.vk.internship.GetResponse> responseObserver)
    {
        try
        {
            String key = request.getKey();
            if (isInvalidKey(key))
            {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT.withDescription("Key cannot be null or empty")
                                .asRuntimeException());
                return;
            }

            byte[] value = repository.get(key);

            com.vk.internship.GetResponse.Builder builder =
                    com.vk.internship.GetResponse.newBuilder();

            if (value != null)
            {
                builder.setValue(ByteString.copyFrom(value));
            }

            responseObserver.onNext(builder.build());

        } catch (Exception e)
        {
            responseObserver.onNext(com.vk.internship.GetResponse.getDefaultInstance());
        } finally
        {
            responseObserver.onCompleted();
        }

    }


    @Override
    public void delete(com.vk.internship.DeleteRequest request,
                       StreamObserver<com.vk.internship.DeleteResponse> responseObserver)
    {
        try
        {
            String key = request.getKey();

            if (isInvalidKey(key))
            {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT.withDescription("Key cannot be null or empty")
                                .asRuntimeException());
                return;
            }

            repository.delete(key);
            responseObserver.onNext(
                    com.vk.internship.DeleteResponse.newBuilder().setSuccess(true).build());

        } catch (Exception e)
        {
            responseObserver.onNext(
                    com.vk.internship.DeleteResponse.newBuilder().setSuccess(false).build());
        } finally
        {
            responseObserver.onCompleted();
        }
    }


    @Override
    public void range(com.vk.internship.RangeRequest request,
                      StreamObserver<com.vk.internship.Entry> responseObserver)
    {
        String keySince = request.getKeySince();
        String keyTo = request.getKeyTo();

        if (isInvalidKey(keySince) || isInvalidKey(keyTo))
        {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                    "KeySince and keyTo cannot be null or empty").asRuntimeException());
            return;
        }

        if (keySince.compareTo(keyTo) > 0)
        {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("KeySince must be <= keyTo")
                            .asRuntimeException());
            return;
        }

        int batchSize = 1000;

        try
        {
            Iterator<Map.Entry<String, byte[]>> it = repository.range(keySince, keyTo, batchSize);
            while (it.hasNext())
            {
                Map.Entry<String, byte[]> entry = it.next();
                com.vk.internship.Entry.Builder builder = com.vk.internship.Entry.newBuilder()
                        .setKey(entry.getKey());
                if (entry.getValue() != null)
                {
                    builder.setValue(ByteString.copyFrom(entry.getValue()));
                }
                responseObserver.onNext(builder.build());
            }
            responseObserver.onCompleted();
        } catch (Exception e)
        {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Range failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void count(com.vk.internship.CountRequest request,
                      StreamObserver<com.vk.internship.CountResponse> responseObserver)
    {
        try
        {
            long count = repository.count();

            responseObserver.onNext(
                    com.vk.internship.CountResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (Exception e)
        {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Failed to get count: " + e.getMessage())
                            .asRuntimeException());
        }

    }

    public void close()
    {
        repository.close();
    }
}
