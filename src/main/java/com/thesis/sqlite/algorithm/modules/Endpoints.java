package com.thesis.sqlite.algorithm.modules;

import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.ExternalServicesClient;
import com.thesis.sqlite.components.spark.DatasetsUtils;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.request.Databases;
import com.thesis.sqlite.entities.Author;
import com.thesis.sqlite.entities.Book;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.core.ParameterizedTypeReference;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.thesis.sqlite.components.spark.DatasetsUtils.authorRenamedColumns;
import static com.thesis.sqlite.components.spark.DatasetsUtils.bookRenamedColumns;
import static org.apache.spark.sql.functions.col;

public class Endpoints extends ImplementationTypeManager {
    private final ExternalServicesClient externalServicesClient;
    private final SparkService sparkService;

    public Endpoints(ExternalServicesClient externalServicesClient, SparkService sparkService) {
        this.externalServicesClient = externalServicesClient;
        this.sparkService = sparkService;
    }

    @Override
    public CompletableFuture<Set<UUID>> findCommonIds(com.thesis.sqlite.dto.request.Endpoints endpoints, Databases databases) {
        // get all author ids from the servers
        CompletableFuture<Dataset<Row>> authorCF = externalServicesClient.getAllPaged(endpoints.authorEndpoints())
                .thenApply(DatasetsUtils::unionAllDatasets);

        CompletableFuture<Dataset<Row>> bookCF = externalServicesClient.getAllPaged(endpoints.bookEndpoints())
                .thenApply(DatasetsUtils::unionAllDatasets);

        return authorCF
                .thenCombine(bookCF, (authorDataset, bookDataset) -> {
                    Dataset<Row> authorsDS = DatasetsUtils.renameDatasetColumns(authorDataset, authorRenamedColumns());
                    Dataset<Row> booksDS =  DatasetsUtils.renameDatasetColumns(bookDataset, bookRenamedColumns());

                    Dataset<Row> rowDataset = sparkService.performJoin(booksDS, authorsDS,
                            (d1, d2) -> d1.col("author_id").equalTo(d2.col("book_author")));

                    return rowDataset.select(col("author_id"))
                            .as(Encoders.STRING())
                            .collectAsList().stream()
                            .map(UUID::fromString)
                            .collect(Collectors.toSet());
                });
    }

    @Override
    public CompletableFuture<Dataset<JoinResult>> finalJoinResult(com.thesis.sqlite.dto.request.Endpoints endpoints, Databases databases, Set<UUID> commonIds) {
        CompletableFuture<Dataset<Row>> authorCF = externalServicesClient
                .getAllPaged(endpoints.authorEndpoints(), commonIds, Author.class, new ParameterizedTypeReference<>() {})
                .thenApply(DatasetsUtils::unionAllDatasets);

        CompletableFuture<Dataset<Row>> bookCF = externalServicesClient
                .getAllPaged(endpoints.bookEndpoints(), commonIds, Book.class, new ParameterizedTypeReference<>() {})
                .thenApply(DatasetsUtils::unionAllDatasets);

        return authorCF
                .thenCombine(bookCF, (authorDataset, bookDataset) -> {
                    Dataset<Row> commonAuthors = authorDataset.withColumnRenamed("id", "author_id");
                    Dataset<Row> commonBooks = bookDataset.withColumnRenamed("author", "book_author");

                    Dataset<Row> rowDataset = sparkService.performJoin(commonBooks, commonAuthors,
                                    (authors, books) -> authors.col("author_id").equalTo(books.col("book_author")))
                            .withColumnRenamed("author_id", "author")
                            .withColumnRenamed("book_author", "bookAuthor");

                    return rowDataset.map((MapFunction<Row, JoinResult>) row -> {
                        Row authorRow = row.getAs("author");
                        Row bookAuthorRow = row.getAs("bookAuthor");
                        Row bookIdRow = row.getAs("id");

                        String authorUUID = String.valueOf(new UUID(authorRow.getLong(1), authorRow.getLong(0)));
                        String bookAuthorUUID = String.valueOf(new UUID(bookAuthorRow.getLong(1), bookAuthorRow.getLong(0)));
                        String bookUUID = String.valueOf(new UUID(bookIdRow.getLong(1), bookIdRow.getLong(0)));

                        return new JoinResult(authorUUID, bookAuthorUUID, row.getAs("name"),
                                row.getAs("title"), bookUUID);
                    }, Encoders.bean(JoinResult.class));

                });
    }
}
