package com.vk.internship;


import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Unit-тестирование работы KVServiceImpl")
class KVServiceImplTest
{
    @Mock
    private KVRepository repository;


    @InjectMocks
    private KVServiceImpl service;


    @Test
    @DisplayName("PUT: Должен вернуть INVALID_ARGUMENT, если ключ некорректный")
    void putShouldReturnErrorWhenKeyIsInvalid()
    {
        PutRequest request = PutRequest.newBuilder().setKey("").build();

        StreamObserver<PutResponse> responseObserver = mock(StreamObserver.class);

        service.put(request, responseObserver);

        verify(responseObserver).onError(argThat(t ->
        {
            Status status = ((StatusRuntimeException) t).getStatus();
            return status.getCode() == Status.Code.INVALID_ARGUMENT;
        }));

        verifyNoInteractions(repository);
    }

    @Test
    @DisplayName("PUT: Должен успешно сохранить данные при корректном ключе")
    void putShouldSucceedWhenKeyIsValid()
    {
        PutRequest request = PutRequest.newBuilder()
                .setKey("valid_key")
                .setValue(ByteString.copyFromUtf8("value")).build();

        StreamObserver<PutResponse> responseObserver = mock(StreamObserver.class);

        when(repository.putAsync(eq("valid_key"), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.put(request, responseObserver);

        verify(repository).putAsync(eq("valid_key"), any(byte[].class));
        verify(responseObserver, timeout(1000)).onNext(argThat(PutResponse::getSuccess));
        verify(responseObserver, timeout(1000)).onCompleted();

    }

    @Test
    @DisplayName("PUT: Должен вернуть ошибку, если репозиторий выбросил исключение")
    void putShouldReturnFailureWhenRepositoryThrowsException()
    {
        PutRequest request = PutRequest.newBuilder().setKey("key").build();

        StreamObserver<PutResponse> responseObserver = mock(StreamObserver.class);

        when(repository.putAsync(eq("key"), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Error")));

        service.put(request, responseObserver);

        verify(responseObserver, timeout(1000)).onError(argThat(t ->
                ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.INTERNAL));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @Test
    @DisplayName("GET: Должен вернуть данные, если ключ существует")
    void getShouldReturnDataWhenKeyExists()
    {
        String key = "existing_key";
        byte[] value = "value".getBytes();

        when(repository.getAsync(key)).thenReturn(CompletableFuture.completedFuture(value));

        GetRequest request = GetRequest.newBuilder().setKey(key).build();

        StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);

        service.get(request, responseObserver);

        verify(responseObserver, timeout(1000)).onNext(argThat(response ->
                response.hasValue() && response.getValue().toStringUtf8().equals("value")));

        verify(responseObserver, timeout(1000)).onCompleted();
    }

    @Test
    @DisplayName("GET: Должен вернуть null, если в базе лежит null")
    void getShouldReturnNullWhenValueIsNull()
    {
        String key = "key";
        when(repository.getAsync(key)).thenReturn(CompletableFuture.completedFuture(null));

        GetRequest request = GetRequest.newBuilder().setKey(key).build();

        StreamObserver<GetResponse> responseObserver = mock(StreamObserver.class);

        service.get(request, responseObserver);

        verify(responseObserver, timeout(1000)).onNext(argThat(response -> !response.hasValue()));
        verify(responseObserver, timeout(1000)).onCompleted();
    }

    @Test
    @DisplayName("RANGE: Должен вернуть ошибку, если keySince > keyTo")
    void rangeShouldReturnErrorWhenSinceGreaterThanTo()
    {
        RangeRequest request = RangeRequest.newBuilder()
                .setKeySince("z")
                .setKeyTo("a")
                .build();

        StreamObserver<Entry> responseObserver = mock(StreamObserver.class);

        service.range(request, responseObserver);


        verify(responseObserver).onError(argThat(t ->
                ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.INVALID_ARGUMENT
        ));
    }


    @Test
    @DisplayName("RANGE: Должен стримить все записи")
    void rangeShouldStreamAllEntries()
    {
        Map.Entry<String, byte[]> entry1 = Map.entry("k1", "v1".getBytes());
        Map.Entry<String, byte[]> entry2 = Map.entry("k2", "v2".getBytes());


        doAnswer(invocation ->
        {
            Consumer<Map.Entry<String, byte[]>> consumer = invocation.getArgument(3);

            consumer.accept(entry1);
            consumer.accept(entry2);

            return CompletableFuture.completedFuture(null);
        }).when(repository).rangeAsync(anyString(), anyString(), anyInt(), any());

        RangeRequest request = RangeRequest.newBuilder()
                .setKeySince("k1")
                .setKeyTo("k2")
                .build();

        StreamObserver<Entry> responseObserver = mock(StreamObserver.class);

        service.range(request, responseObserver);

        verify(responseObserver, timeout(1000).times(2)).onNext(any(Entry.class));
        verify(responseObserver, timeout(1000)).onCompleted();
    }


    @Test
    @DisplayName("DELETE: Должен успешно удалить ключ")
    void deleteShouldSucceed()
    {
        String key = "to_delete";
        DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
        StreamObserver<DeleteResponse> responseObserver = mock(StreamObserver.class);

        when(repository.deleteAsync(key)).thenReturn(CompletableFuture.completedFuture(null));

        service.delete(request, responseObserver);

        verify(repository).deleteAsync(key);
        verify(responseObserver, timeout(1000)).onNext(argThat(DeleteResponse::getSuccess));
        verify(responseObserver, timeout(1000)).onCompleted();
    }


    @Test
    @DisplayName("COUNT: Должен вернуть текущее количество записей")
    void countShouldReturnCorrectValue()
    {
        long cnt = 125;
        when(repository.countAsync()).thenReturn(CompletableFuture.completedFuture(cnt));

        CountRequest request = CountRequest.newBuilder().build();
        StreamObserver<CountResponse> responseObserver = mock(StreamObserver.class);

        service.count(request, responseObserver);

        verify(responseObserver, timeout(1000)).onNext(argThat(r -> r.getCount() == cnt));
        verify(responseObserver, timeout(1000)).onCompleted();
    }

}
