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
              VALUE_FORMAT='JSON',
              PARTITIONS=1,
              REPLICAS=1
            );
        """).get();

        // Create INVENTORY table
        client.executeStatement("""
            CREATE TABLE IF NOT EXISTS inventory (
              product_id INT PRIMARY KEY,
              stock_level INT
            ) WITH (
              KAFKA_TOPIC='inventory',
              VALUE_FORMAT='JSON',
              PARTITIONS=1,
              REPLICAS=1
            );
        """).get();

        // Create the JOIN stream
        client.executeStatement("""
            CREATE STREAM IF NOT EXISTS enriched_orders AS
              SELECT o.product_id,
                     o.quantity,
                     i.stock_level
              FROM orders o
              LEFT JOIN inventory i ON o.product_id = i.product_id
              EMIT CHANGES;
        """).get();

        client.executeStatement("DROP TABLE IF EXISTS enriched_orders_table;");

        client.executeStatement("""
                CREATE TABLE enriched_orders_table AS
                    SELECT
                      O.PRODUCT_ID AS PRODUCT_ID,
                      SUM(O.QUANTITY) AS TOTAL_QUANTITY,
                      MAX(I.STOCK_LEVEL) AS STOCK_LEVEL
                    FROM orders O
                    LEFT JOIN inventory I ON O.PRODUCT_ID = I.PRODUCT_ID
                    GROUP BY O.PRODUCT_ID
                    EMIT CHANGES;
         """).get();

    }

    public void streamEnrichedOrders() throws ExecutionException, InterruptedException {
        client.streamQuery("SELECT * FROM enriched_orders_table EMIT CHANGES;")
                .thenAccept(streamedResult -> streamedResult.subscribe(new Subscriber<Row>() {
                    private Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Row row) {
                        try {
                            String product = row.getValue("PRODUCT_ID").toString();
                            Integer quantity = row.getInteger("TOTAL_QUANTITY"); // match table column
                            Integer stock = row.getInteger("STOCK_LEVEL");
                            System.out.println("Enriched order → product=" + product +
                                    " quantity=" + quantity +
                                    " stock=" + stock);
                        } catch (Exception ex) {
                            System.err.println("Failed to parse row: " + ex.getMessage());
                        }
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
                }));

    }
}
