package com.thesis.sqlite.algorithm.modules;

import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.ExternalServicesClient;
import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.query.UtilsQuery;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.request.Databases;
import com.thesis.sqlite.dto.request.Endpoints;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Advanced extends ImplementationTypeManager {
    @Value( "${spring.datasource.url}" )
    private String jdbcUrl;
    private static final String MY_TABLE = "author"; //TODO k : add it as conf
    private final ExternalServicesClient externalServicesClient;
    private final SparkService sparkService;
    private final ExternalInteractor externalInteractor;
    private final NodesInfoManager nodesInfoManager;

    public Advanced(ExternalServicesClient externalServicesClient, SparkService sparkService, ExternalInteractor externalInteractor, NodesInfoManager nodesInfoManager) {
        this.externalServicesClient = externalServicesClient;
        this.sparkService = sparkService;
        this.externalInteractor = externalInteractor;
        this.nodesInfoManager = nodesInfoManager;
    }

    public CompletableFuture<Dataset<Row>> performAlgorithm(String sqlQuery) {
        ArrayList<String> joins = UtilsQuery.getJoinStr(sqlQuery);
        HashMap<String, String> aliasMap = UtilsQuery.getAliasMap(sqlQuery);

        Map<String, String> collect = aliasMap.keySet().stream()
                .map(s -> new Pair<>(s, Optional.ofNullable(nodesInfoManager.tableInfos.get(s))))
                .filter(entry -> entry.getValue().isPresent()) //TODO k: do not execute query if we do not have infos for a table
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().get()))
                .collect(Collectors.toMap(Pair::getKey, p -> p.getValue().getUrl()));

        Properties properties = new Properties();

        // STEP 1: Store local table to Spark
        Optional.of(MY_TABLE)
                .filter(collect::containsKey)
                .ifPresent(table -> {
                    Dataset<Row> local = sparkService.getSpark().read().jdbc(jdbcUrl, MY_TABLE, properties);
                    local.createOrReplaceTempView(table);
                });


        //external
        return Future.allOf(collect.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(MY_TABLE))
                        .map(entry -> externalInteractor.getAllPaged(sparkService, entry.getValue(), entry.getKey())
                                .thenAccept(dataset -> dataset.createOrReplaceTempView(entry.getKey())))
                        .toList())
                .thenApply(__ -> sparkService.getSpark().sql(sqlQuery));
    }

    @Override
    public CompletableFuture<Set<UUID>> findCommonIds(Endpoints endpoints, Databases databases) {
        return null;
    }

    @Override
    public CompletableFuture<Dataset<JoinResult>> finalJoinResult(Endpoints endpoints, Databases databases, Set<UUID> commonIds) {
        return null;
    }
}
