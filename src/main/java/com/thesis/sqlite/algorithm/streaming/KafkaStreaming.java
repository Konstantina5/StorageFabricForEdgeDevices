package com.thesis.sqlite.algorithm.streaming;
import io.confluent.ksql.api.client.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
public class KafkaStreaming {
    private final Client client;

    @Autowired
    public KafkaStreaming(Client client) {
        this.client = client;
    }

    public void createStreamsAndTables() throws ExecutionException, InterruptedException, ExecutionException {
        // Create ORDERS stream
        client.executeStatement("""
            CREATE STREAM IF NOT EXISTS orders (
              product_id INT,
              quantity INT
            ) WITH (
              KAFKA_TOPIC='orders',
              VALUE_FORMAT='JSON'
            );
        """).get();

        // Create INVENTORY table
        client.executeStatement("""
            CREATE TABLE IF NOT EXISTS inventory (
              product_id INT PRIMARY KEY,
              stock_level INT
            ) WITH (
              KAFKA_TOPIC='inventory',
              VALUE_FORMAT='JSON'
            );
        """).get();

        // Create the JOIN stream
        client.executeStatement("""
            CREATE STREAM IF NOT EXISTS enriched_orders AS
              SELECT o.product_id,
                     o.quantity,
                     o.price,
                     i.stock_level
              FROM orders o
              LEFT JOIN inventory i
                ON o.product_id = i.product_id
              EMIT CHANGES;
        """).get();

    }

    public void streamEnrichedOrders() throws ExecutionException, InterruptedException {
        client.streamQuery("SELECT * FROM enriched_orders EMIT CHANGES;")
                .thenAccept(streamedResult -> {
                    streamedResult.subscribe(new Subscriber<Row>() {
                        private Subscription subscription;

                        @Override
                        public void onSubscribe(Subscription s) {
                            this.subscription = s;
                            s.request(Long.MAX_VALUE); // request all rows
                        }

                        @Override
                        public void onNext(Row row) {
                            // minimal row processing
                            String product = String.valueOf(row.getValue("PRODUCT_ID"));
                            int quantity = ((Number) row.getValue("QUANTITY")).intValue();
                            int stock = row.getValue("STOCK_LEVEL") == null
                                    ? -1
                                    : ((Number) row.getValue("STOCK_LEVEL")).intValue();

                            System.out.println("Enriched order → product=" + product +
                                    " quantity=" + quantity +
                                    " stock=" + stock);
                        }

                        @Override
                        public void onError(Throwable t) {
                            System.err.println("Stream error: " + t.getMessage());
                            t.printStackTrace();
                        }

                        @Override
                        public void onComplete() {
                            System.out.println("Stream completed.");
                        }
                    });
                })
                .exceptionally(e -> {
                    System.err.println("Failed to start streamed query: " + e.getMessage());
                    e.printStackTrace();
                    return null;
                });
    }
}
