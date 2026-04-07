# KV Storage Service (gRPC)

Тестовое задание на позицию Java-разработчика


## Стек

* Java 25
* gRPC, Protobuf
* Tarantool Java SDK 1.5
* Docker & Docker Compose
* JUnit5, Mockito & Testcontainers
* JMH (Java Microbenchmark Harness)
* SLF4J



## Архитектура

1. Работа с Tarantool вынесена в слой `KVRepository`
2. Обработка `null-value` реализована с помощью `optional` полей в Protobuf
3. Взаимодействие с Tarantool-ом реализовано в асинхронном виде.
4. Метод `Range` реализован через загрузка `batch`-ами заданного размера (1000), возвращаются же пользователю данные
   через gRPC-stream.



## Результаты бенчмарков

Тестирование проводилось на заполненной базе объемом **5 000 000 записей**.

**Multi-batch** — это сценарий, при котором размер запрашиваемого диапазона превышает `batchSize`,
что заставляет систему выполнять несколько последовательных запросов для подгрузки данных.

### 1. Сценарий: 1 поток

| Метод                 | Batch Size | Latency (Avg) | Throughput (Total) |
|:----------------------|:----------:|:--------------|:-------------------|
| GET (Fixed Key)       |     —      | 99.97 µs/op   | **10 003 ops/s**   |
| GET (Random Key)      |     —      | 125.74 µs/op  | **7 952 ops/s**    |
| PUT (Random Key)      |     —      | 162.75 µs/op  | **6 144 ops/s**    |
| RANGE (Fixed Window)  |    500     | 396.16 µs/op  | **2 524 ops/s**    |
| RANGE (Fixed Window)  |    1000    | 564.00 µs/op  | **1 773 ops/s**    |
| RANGE (Fixed Window)  |    2000    | 910.26 µs/op  | **1 098 ops/s**    |
| RANGE (Fixed Window)  |    5000    | 2.16 ms/op    | **462 ops/s**      |
| RANGE (Random Window) |    500     | 392.55 µs/op  | **2 547 ops/s**    |
| RANGE (Random Window) |    1000    | 571.97 µs/op  | **1 748 ops/s**    |
| RANGE (Random Window) |    2000    | 983.60 µs/op  | **1 016 ops/s**    |
| RANGE (Random Window) |    5000    | 2.77 ms/op    | **361 ops/s**      |
| RANGE (Multi-batch)   |    500     | 1.34 ms/op    | **745 ops/s**      |
| RANGE (Multi-batch)   |    1000    | 1.91 ms/op    | **522 ops/s**      |
| RANGE (Multi-batch)   |    2000    | 3.29 ms/op    | **303 ops/s**      |
| RANGE (Multi-batch)   |    5000    | 7.60 ms/op    | **131 ops/s**      |

### 2. Сценарий: 8 потоков

| Метод                 | Batch Size | Latency (Avg) | Throughput (Total) |
|:----------------------|:----------:|:--------------|:-------------------|
| GET (Fixed Key)       |     —      | 197.0 µs/op   | **40 964 ops/s**   |
| GET (Random Key)      |     —      | 192.0 µs/op   | **38 398 ops/s**   |
| PUT (Random Key)      |     —      | 259.0 µs/op   | **34 132 ops/s**   |
| RANGE (Fixed Window)  |    500     | 1.0 ms/op     | **9 108 ops/s**    |
| RANGE (Fixed Window)  |    1000    | 2.0 ms/op     | **5 071 ops/s**    |
| RANGE (Fixed Window)  |    2000    | 3.0 ms/op     | **2 656 ops/s**    |
| RANGE (Fixed Window)  |    5000    | 9.0 ms/op     | **897 ops/s**      |
| RANGE (Random Window) |    500     | 1.0 ms/op     | **6 995 ops/s**    |
| RANGE (Random Window) |    1000    | 2.0 ms/op     | **4 248 ops/s**    |
| RANGE (Random Window) |    2000    | 4.0 ms/op     | **2 118 ops/s**    |
| RANGE (Random Window) |    5000    | 11.0 ms/op    | **702 ops/s**      |
| RANGE (Multi-batch)   |    500     | 3.0 ms/op     | **2 358 ops/s**    |
| RANGE (Multi-batch)   |    1000    | 7.0 ms/op     | **1 272 ops/s**    |
| RANGE (Multi-batch)   |    2000    | 12.0 ms/op    | **658 ops/s**      |
| RANGE (Multi-batch)   |    5000    | 37.0 ms/op    | **230 ops/s**      |

## Запуск проекта

### 1. Быстрый старт через (Docker-compose)

Команда соберет JAR и запустит сервис вместе с Tarantool-ом, инициализированным скриптом `tarantool/init.lua`.

```bash
docker-compose up --build -d
```

### 2. Запуск тестов (Unit + Integration)

Интеграционные тесты сами поднимут Tarantool через Testcontainers

```bash
mvn clean verify
```

### 3. Запуск бенчмарков

Бенчмарки запускаются как отдельное приложение. Для получения правильных результатов требуется поднятый вне
docker-контейнера Tarantool, инициализированный скриптом `tarantool/init.lua`.

```bash
mvn clean package -DskipTests
java -jar target/benchmarks.jar
```
    