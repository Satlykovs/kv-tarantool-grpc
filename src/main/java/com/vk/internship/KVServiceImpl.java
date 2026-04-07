package com.vk.internship;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase
{
    private static final String BAD_KEY_MESSAGE = "Key cannot be null or empty";
    private final KVRepository repository;
    private final Logger logger = LoggerFactory.getLogger(KVServiceImpl.class);

    public KVServiceImpl(KVRepository repository)
    {
        this.repository = repository;
    }

    private boolean isInvalidKey(String key)
    {
        return key == null || key.trim().isEmpty();
    }

    @Override
    public void put(PutRequest request,
                    StreamObserver<PutResponse> responseObserver)
    {
        String key = request.getKey();
        if (isInvalidKey(key))
        {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(BAD_KEY_MESSAGE)
                            .asRuntimeException());
            return;
        }


        byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;

        repository.putAsync(key, value).whenComplete((result, ex) ->
        {
            if (ex != null)
            {
                logger.error("Failed to PUT key '{} : {}", key, ex.getMessage(), ex);
                responseObserver.onError(Status.INTERNAL.withCause(ex).asRuntimeException());
            } else
            {
                responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        });

    }


    @Override
    public void get(com.vk.internship.GetRequest request,
                    StreamObserver<com.vk.internship.GetResponse> responseObserver)
    {

        String key = request.getKey();
        if (isInvalidKey(key))
        {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(BAD_KEY_MESSAGE)
                            .asRuntimeException());
            return;
        }


        repository.getAsync(key).whenComplete((value, ex) ->
        {
            if (ex != null)
            {
                logger.error("Failed to GET key: '{}' : {}", key, ex.getMessage(), ex);
                responseObserver.onError(Status.INTERNAL.withCause(ex).asRuntimeException());
            } else
            {
                GetResponse.Builder builder = GetResponse.newBuilder();
                if (value != null) builder.setValue(ByteString.copyFrom(value));
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }
        });

    }


    @Override
    public void delete(com.vk.internship.DeleteRequest request,
                       StreamObserver<com.vk.internship.DeleteResponse> responseObserver)
    {

        String key = request.getKey();

        if (isInvalidKey(key))
        {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(BAD_KEY_MESSAGE)
                            .asRuntimeException());
            return;
        }


        repository.deleteAsync(key).whenComplete((result, ex) ->
        {
            if (ex != null)
            {
                logger.error("Failed to DELETE key '{}' : {}", key, ex.getMessage(), ex);
                responseObserver.onError(Status.INTERNAL.withCause(ex).asRuntimeException());
            } else
            {
                responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        });

    }


    @Override
    public void range(RangeRequest request,
                      StreamObserver<Entry> responseObserver)
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

        logger.info("Starting async range scan from '{}' to '{}'", keySince, keyTo);

        int batchSize = 1000;
        repository.rangeAsync(keySince, keyTo, batchSize, entry ->
                {
                    Entry.Builder protoEntryBuilder = Entry.newBuilder()
                            .setKey(entry.getKey());
                    if (entry.getValue() != null)
                    {
                        protoEntryBuilder.setValue(ByteString.copyFrom(entry.getValue()));
                    }

                    responseObserver.onNext(protoEntryBuilder.build());
                })
                .whenComplete((v, ex) ->
                {
                    if (ex != null)
                    {
                        logger.error("Range request failed: {}", ex.getMessage(), ex);
                        responseObserver.onError(
                                Status.INTERNAL.withCause(ex).asRuntimeException());
                    } else
                    {
                        responseObserver.onCompleted();
                    }
                });


    }

    @Override
    public void count(com.vk.internship.CountRequest request,
                      StreamObserver<com.vk.internship.CountResponse> responseObserver)
    {
        repository.countAsync().whenComplete((count, ex) ->
        {
            if (ex != null)
            {
                logger.error("Failed to get count: {}", ex.getMessage(), ex);
                responseObserver.onError(Status.INTERNAL.withCause(ex).asRuntimeException());
            } else
            {
                responseObserver.onNext(CountResponse.newBuilder().setCount(count).build());
                responseObserver.onCompleted();
            }
        });

    }

    public void close()
    {
        repository.close();
    }
}
