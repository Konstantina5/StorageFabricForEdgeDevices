package com.thesis.sqlite.controllers;

import com.thesis.sqlite.algorithm.streaming.KafkaStreaming;
import com.thesis.sqlite.components.streaming.GenerateData;
import com.thesis.sqlite.dto.request.streaming.QueryTable;
import com.thesis.sqlite.dto.request.streaming.QueryWithDataAmount;
import com.thesis.sqlite.dto.streaming.StreamingResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.TableInfo;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@RestController
@ConditionalOnProperty(
        value="streaming",
        havingValue = "true")
@RequestMapping("api/streaming")
public class StreamingController {

    private final Client client;
    private final KafkaStreaming kafkaStreaming;
    private final GenerateData generateData;

    @Autowired
    public StreamingController(Client client, KafkaStreaming kafkaStreaming, GenerateData generateData) {
        this.client = client;
        this.kafkaStreaming = kafkaStreaming;
        this.generateData = generateData;
    }

    @PostMapping("/query/pull")
    public CompletableFuture<List<?>> getPullResult(@RequestBody String query) throws Exception {
        List<TableInfo> tableInfos = client.listTables().get();

        List<Row> rows = client.executeQuery(query).get();
        System.out.println(rows.isEmpty());

        return client.executeQuery(query)
                .thenApply(result -> {
                    if (result == null) return List.of();  // return empty list if no rows
                    return result;
                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return List.of(); // fallback empty list
                });
    }

    @PostMapping("/query/pull/with_init")
    public CompletableFuture<ResponseEntity<?>> getPullResultWithInit(@RequestBody QueryWithDataAmount queryTable) throws Exception {
        List<TableInfo> tableInfos = client.listTables().get();
        long start = Instant.now().toEpochMilli();
        return kafkaStreaming.createQueryTable(queryTable)
                .thenCompose(__ -> kafkaStreaming.sendDataForAllTablesInTheQuery(queryTable.getQuery(), queryTable.getDataAmount()))
                .thenCompose(__ -> {
                            long endTransfer = Instant.now().toEpochMilli();
                            return client.executeQuery(String.format("SELECT * FROM %s;", queryTable.getTableName()))
                                    .thenApply(result -> {
                                        long end = Instant.now().toEpochMilli();

                                        StreamingResult streamingResult = Optional.ofNullable(result)
                                                .map(List::size)
                                                .map(size -> new StreamingResult(end - endTransfer, endTransfer - start,
                                                        result.size(), end - start))
                                                .orElseGet(() -> new StreamingResult(end - endTransfer, endTransfer - start,
                                                        0, end - start));

                                        return com.thesis.sqlite.results.Client.Results.ok(streamingResult);
                                    });
                        }
                )
                .handle((res, ex) -> {
                    if (ex != null) {
                        return com.thesis.sqlite.results.Client.Errors.internalServerError(ex.getMessage());
                    }
                    return res;
                });
    }

    @GetMapping("/query/push")
    public CompletableFuture<Void> getPushResult(@RequestParam String tableName) {
        return kafkaStreaming.getPushQueryResults(tableName);
    }

    @PostMapping("/pull/initialization")
    public CompletableFuture<List<Row>> get(QueryTable queryTable) throws Exception {
        return kafkaStreaming.createQueryTable(queryTable)
                .thenCompose(res -> client.executeQuery(queryTable.getQuery()))
                .exceptionally(e -> {
                    e.printStackTrace();
                    return List.of(); // fallback empty list
                });
    }

    @PostMapping("/generate_data")
    public ResponseEntity<?> generateData(Integer dataAmount) {
        generateData.sendBatch(Optional.ofNullable(dataAmount));
        return com.thesis.sqlite.results.Client.Results.ok();
    }

    @GetMapping(value = "/stream/revenue", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamRevenue(String query) throws Exception {
        return Flux.create(sink -> {
            client.streamQuery(query + " EMIT CHANGES;")
                    .thenAccept(streamedResult -> streamedResult.subscribe(new Subscriber<Row>() {
                        private Subscription subscription;

                        @Override
                        public void onSubscribe(Subscription s) {
//                            s.request(10);
                            this.subscription = s;
                            s.request(100);
                        }

                        @Override
                        public void onNext(Row row) {
                            System.out.println(row.values().toString());
                            sink.next(row.values().toString());
                        }

                        @Override
                        public void onError(Throwable t) {
                            System.err.println("Stream error: " + t.getMessage());
                            t.printStackTrace();
                        }

                        @Override
                        public void onComplete() {
                            sink.complete();
                            System.out.println("Stream completed.");
                        }
                    }));
        });
    }
}
