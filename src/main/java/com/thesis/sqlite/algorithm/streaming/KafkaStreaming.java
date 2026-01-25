package com.thesis.sqlite.algorithm.streaming;

import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.streaming.GenerateData;
import com.thesis.sqlite.components.streaming.StreamingTemplateService;
import com.thesis.sqlite.dto.request.streaming.QueryTable;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.thesis.sqlite.utils.Utils.TABLE_NAME;

@Component
@ConditionalOnProperty(
        value="streaming",
        havingValue = "true")
public class KafkaStreaming {
    private final Client client;
    private final StreamingTemplateService streamingTemplateService;
    private final NodesInfoManager infoManager;
    private final ExternalInteractor externalInteractor;
    private final GenerateData generateData;

    @Autowired
    public KafkaStreaming(Client client, StreamingTemplateService streamingTemplateService, NodesInfoManager infoManager,
                          ExternalInteractor externalInteractor, GenerateData generateData) {
        this.client = client;
        this.streamingTemplateService = streamingTemplateService;
        this.infoManager = infoManager;
        this.externalInteractor = externalInteractor;
        this.generateData = generateData;
    }

    public CompletableFuture<ExecuteStatementResult> initialize() {
        return CompletableFuture.supplyAsync(streamingTemplateService::loadKafkaStreamTemplate)
                .thenCompose(str -> client.executeStatement(str));
    }

    public CompletableFuture<List<ResponseEntity<Void>>> sendDataForAllTablesInTheQuery(String query, Integer dataAmount) {
        Set<String> tables = getTableMap(query);

        boolean b = tables.stream()
//                .filter(key -> !key.equals(Utils.TABLE_NAME)) TODO k: uncomment when QueryHandler is ready
                .allMatch(infoManager.getTableInfos()::containsKey);

        if (!b) {
            throw new RuntimeException("Not all tables are present, cannot execute query");
        }

        generateData.sendBatch(Optional.ofNullable(dataAmount));

        Map<String, String> collect = tables.stream()
                .filter(table -> !table.equals(TABLE_NAME))
                .map(s -> new Pair<>(s, Optional.ofNullable(infoManager.getTableInfos().get(s))))
                .filter(entry -> entry.getValue().isPresent())
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().get()))
                .collect(Collectors.toMap(Pair::getKey, p -> p.getValue().getUrl()));

        return Future.allOf(collect.values().stream()
                .map(url -> externalInteractor.pushDataToKafka(url, dataAmount))
                .toList());
    }

    public CompletableFuture<ExecuteStatementResult> createQueryTable(QueryTable queryTable) {
        return  client.executeStatement(String.format("DROP TABLE IF EXISTS %s;", queryTable.getTableName()))
                .thenCompose(__ -> client.executeStatement(String.format("""
                    CREATE TABLE %s AS %s
                    """, queryTable.getTableName(), queryTable.getQuery())));
    }

    public CompletableFuture<Void> getPushQueryResults(String tableName) {
        return client.streamQuery(String.format("SELECT * FROM %s EMIT CHANGES;", tableName))
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());
                    RowSubscriber subscriber = new RowSubscriber();
                    streamedQueryResult.subscribe(subscriber);
                }).exceptionally(e -> {
                    System.out.println("Request failed: " + e);
                    return null;
                });
    }

    private static Set<String> getTableMap(String query) {
        String lower = query.toLowerCase();

        int fromIdx = lower.indexOf("from");
        if (fromIdx == -1) {
            return Set.of();
        }

        int whereIdx = lower.indexOf("where", fromIdx);
        int groupByIdx = lower.indexOf("group by", fromIdx);

        int endIdx;
        if (whereIdx != -1 && groupByIdx != -1) {
            endIdx = Math.min(whereIdx, groupByIdx);
        } else if (whereIdx != -1) {
            endIdx = whereIdx;
        } else if (groupByIdx != -1) {
            endIdx = groupByIdx;
        } else {
            endIdx = query.length();
        }

        String aliases = query.substring(fromIdx + 4, endIdx).trim();
        String[] aliasArr = aliases.split(",");

        return Arrays.stream(aliasArr)
                .map(String::trim)
                .collect(Collectors.toSet());
    }

}
