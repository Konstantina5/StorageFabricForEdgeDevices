package com.thesis.sqlite.algorithm.modules;

import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.spark.DatasetsUtils;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.request.Databases;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import com.thesis.sqlite.dto.request.Endpoints;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class Database extends ImplementationTypeManager {
    private final SparkService sparkService;

    public Database(SparkService sparkService) {
        this.sparkService = sparkService;
    }

    @Override
    public CompletableFuture<Dataset<JoinResult>> finalJoinResult(Endpoints endpoints, Databases databases, Set<UUID> commonIds) {
        String[] uuidStrings = commonIds.stream()
                .map(UUID::toString)
                .toArray(String[]::new);

        Dataset<Row> commonAuthors = DatasetsUtils.unionAllDatasets(databases.authorDatabases().stream()
                        .map(url -> sparkService.getSpark().read().jdbc(url, "author", getPropertiesFunc.apply(url)))
                        .map(dataset -> dataset.where(col("id").isin((Object[]) uuidStrings)))
                        .toList())
                .withColumnRenamed("id", "author");

        Dataset<Row> commonBooks = DatasetsUtils.unionAllDatasets(databases.bookDatabases().stream()
                        .map(url -> sparkService.getSpark().read().jdbc(url, "book", getPropertiesFunc.apply(url)))
                        .map(dataset -> dataset.where(col("author").isin((Object[]) uuidStrings)))
                        .toList())
                .withColumnRenamed("author", "bookAuthor")
                .withColumnRenamed("id", "book_id");

        Dataset<Row> rowDataset = sparkService.performJoin(commonBooks, commonAuthors,
                (d1, d2) -> d1.col("author").equalTo(d2.col("bookAuthor")));

        Dataset<JoinResult> result = rowDataset.map((MapFunction<Row, JoinResult>) row -> {
            String author = row.getAs("author");
            String bookAuthor = row.getAs("bookAuthor");

            return new JoinResult(author, bookAuthor, row.getAs("name"), row.getAs("title"),
                    row.getAs("book_id"));
        }, Encoders.bean(JoinResult.class));

        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Set<UUID>> findCommonIds(Endpoints endpoints, Databases databases) {
        Dataset<Row> authorsDS = DatasetsUtils.unionAllDatasets(databases.authorDatabases().stream()
                        .map(url -> sparkService.getSpark().read().jdbc(url, "author", getPropertiesFunc.apply(url)))
                        .map(dataset -> dataset
                                .groupBy(col("id"))
                                .agg(count("id").alias("set1"))
                                .orderBy(col("id")))
                        .toList());

        Dataset<Row> booksDS = DatasetsUtils.unionAllDatasets(databases.bookDatabases().stream()
                        .map(url -> sparkService.getSpark().read().jdbc(url, "book", getPropertiesFunc.apply(url)))
                        .map(dataset -> dataset
                                .groupBy(col("author"))
                                .agg(count("author").alias("set2"))
                                .orderBy(col("author")))
                        .toList());

        Dataset<Row> rowDataset = sparkService.performJoin(booksDS, authorsDS,
                (d1, d2) -> d1.col("id").equalTo(d2.col("author")));

        Set<UUID> ids = rowDataset.select(col("id"))
                .as(Encoders.STRING())
                .collectAsList().stream()
                .map(UUID::fromString)
                .collect(Collectors.toSet());
        return CompletableFuture.completedFuture(ids);

    }

    private static final Function<String, Properties> getPropertiesFunc = (in) -> {
        Properties properties = new Properties();
        properties.putIfAbsent("user", "postgres");
        properties.putIfAbsent("password", "postgres");
        return properties;
    };
}
