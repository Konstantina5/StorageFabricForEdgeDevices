package com.thesis.sqlite.controllers;

import com.thesis.sqlite.algorithm.streaming.KafkaStreaming;
import com.thesis.sqlite.dto.request.streaming.QueryTable;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.TableInfo;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@ConditionalOnProperty(
        value="streaming",
        havingValue = "true")
@RequestMapping("api/streaming")
public class StreamingController {

    private final Client client;
    private final KafkaStreaming kafkaStreaming;

    @Autowired
    public StreamingController(Client client, KafkaStreaming kafkaStreaming) {
        this.client = client;
        this.kafkaStreaming = kafkaStreaming;
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

    @GetMapping("/query/push")
    public CompletableFuture<Void> getPushResult(@RequestParam String tableName) {
        return kafkaStreaming.getPushQueryResults(tableName);
    }

    @PostMapping("/pull/initialization")
    public CompletableFuture<List<Row>> get(QueryTable queryTable) throws Exception {
        return kafkaStreaming.createQueryTable(queryTable)
                .thenCompose(res -> client.executeQuery(queryTable.query()))
                .exceptionally(e -> {
                    e.printStackTrace();
                    return List.of(); // fallback empty list
                });
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
//                            try {
//                                String product = row.getValue("PRODUCT_ID").toString();
//                                Integer quantity = row.getInteger("TOTAL_QUANTITY"); // match table column
//                                Integer stock = row.getInteger("STOCK_LEVEL");
//                                System.out.println("Enriched order → product=" + product +
//                                        " quantity=" + quantity +
//                                        " stock=" + stock);
//                            } catch (Exception ex) {
//                                System.err.println("Failed to parse row: " + ex.getMessage());
//                            }
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
