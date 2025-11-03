package com.thesis.sqlite.controllers;

import com.thesis.sqlite.algorithm.streaming.KafkaStreaming;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TableInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import io.confluent.ksql.api.client.Client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RestController
@RequestMapping("api/streaming")
public class StreamingController {

    private final Client client;
    private final KafkaStreaming kafkaStreaming;

    @Autowired
    public StreamingController(Client client, KafkaStreaming kafkaStreaming) {
        this.client = client;
        this.kafkaStreaming = kafkaStreaming;
    }

    @GetMapping("/orders/enriched/pull")
    public CompletableFuture<List<?>> getEnrichedOrders() throws Exception {
        var query = "SELECT * FROM ENRICHED_ORDERS_TABLE;"; // Pull query

        List<TableInfo> tableInfos = client.listTables().get();

        List<Row> rows = client.executeQuery(query).get();
        System.out.println(rows.isEmpty());

        return client.executeQuery(query)
                .thenApply(result -> {
                    if (result == null) return List.of();  // return empty list if no rows
                    return result.stream()
                            .map(row -> new EnrichedOrder(
                                    row.getInteger("PRODUCT_ID"),
                                    row.getInteger("TOTAL_QUANTITY"),
                                    row.getInteger("STOCK_LEVEL")
                            ))
                            .collect(Collectors.toList());
                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return List.of(); // fallback empty list
                });
    }

    @GetMapping("/orders/enriched")
    public void getEnriched() throws Exception {
        kafkaStreaming.streamEnrichedOrders();
    }

    // Update your record accordingly
    public record EnrichedOrder(Integer productId, Integer totalQuantity, Integer stockLevel) {}
}
