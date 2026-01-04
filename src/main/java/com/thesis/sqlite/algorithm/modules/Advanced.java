package com.thesis.sqlite.algorithm.modules;

import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.query.UtilsQuery;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import com.thesis.sqlite.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.thesis.sqlite.algorithm.Views.rewriteFromClause;

public class Advanced extends ImplementationTypeManager {
    @Value( "${spring.datasource.url}" )
    private String jdbcUrl;
    private static final String MY_TABLE = Utils.TABLE_NAME;
    private final SparkService sparkService;
    private final ExternalInteractor externalInteractor;
    private final NodesInfoManager nodesInfoManager;

    public Advanced(SparkService sparkService, ExternalInteractor externalInteractor, NodesInfoManager nodesInfoManager) {
        this.sparkService = sparkService;
        this.externalInteractor = externalInteractor;
        this.nodesInfoManager = nodesInfoManager;
    }

    public CompletableFuture<Dataset<Row>> performAlgorithm(String sqlQuery) {
        HashMap<String, String> aliasMap = UtilsQuery.getAliasMap(sqlQuery);

        boolean b = aliasMap.keySet().stream()
//                .filter(key -> !key.equals(Utils.TABLE_NAME)) TODO k: uncomment when QueryHandler is ready
                .allMatch(nodesInfoManager.getTableInfos()::containsKey);
        if (!b) {
            throw new RuntimeException("Not all tables are present, cannot execute query");
        }

        Map<String, String> collect = aliasMap.keySet().stream()
                .map(s -> new Pair<>(s, Optional.ofNullable(nodesInfoManager.tableInfos.get(s))))
                .filter(entry -> entry.getValue().isPresent())
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().get()))
                .collect(Collectors.toMap(Pair::getKey, p -> p.getValue().getUrl()));

        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Properties properties = new Properties();
        properties.setProperty("driver", "org.sqlite.JDBC");

        // STEP 1: Store local table to Spark
        String localView = "(" + UtilsQuery.getLocalView(MY_TABLE, aliasMap.get(MY_TABLE), sqlQuery) + ") as " + aliasMap.get(MY_TABLE);
        Optional.of(MY_TABLE)
                .filter(collect::containsKey)
                .ifPresent(table -> {
                    Dataset<Row> local = sparkService.getSpark().read().jdbc(jdbcUrl, localView, properties);
                    local.createOrReplaceTempView(aliasMap.get(table));
                });


        //external
        return Future.allOf(collect.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(MY_TABLE))
                        .map(entry -> externalInteractor.getAllPaged(sparkService, entry.getValue(), sqlQuery)
                                .thenAccept(dataset -> dataset.createOrReplaceTempView(aliasMap.get(entry.getKey()))))
                        .toList())
                .thenApply(__ -> sparkService.getSpark().sql(rewriteFromClause(sqlQuery)));
    }

    @Override
    public String getLocalView(String sqlQuery) {
        HashMap<String, String> aliasMap = UtilsQuery.getAliasMap(sqlQuery);

        return  UtilsQuery.getLocalView(MY_TABLE, aliasMap.get(MY_TABLE), sqlQuery);
    }

}
