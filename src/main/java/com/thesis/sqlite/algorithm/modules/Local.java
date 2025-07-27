package com.thesis.sqlite.algorithm.modules;


import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.spark.DatasetsUtils;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.request.Databases;
import com.thesis.sqlite.dto.request.Endpoints;
import com.thesis.sqlite.entities.Author;
import com.thesis.sqlite.entities.Book;
import com.thesis.sqlite.services.AuthorService;
import com.thesis.sqlite.services.BookService;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.thesis.sqlite.components.spark.DatasetsUtils.authorRenamedColumns;
import static com.thesis.sqlite.components.spark.DatasetsUtils.bookRenamedColumns;
import static org.apache.spark.sql.functions.col;

public class Local extends ImplementationTypeManager {
    private final BookService bookService;
    private final AuthorService authorService;
    private final SparkService sparkService;

    public Local(BookService bookService, AuthorService authorService, SparkService sparkService) {
        this.bookService = bookService;
        this.authorService = authorService;
        this.sparkService = sparkService;
    }

    @Override
    public CompletableFuture<Set<UUID>> findCommonIds(Endpoints endpoints, Databases databases) {
        Pageable pageable = PageRequest.of(0, 1000);

        Dataset<Row> authors = DatasetsUtils.createDataset(sparkService.getSpark(),
                        authorService::findAuthorGrouped, pageable, Encoders.bean(BookAuthorCount.class))
                .toDF();
        Dataset<Row> books = DatasetsUtils.createDataset(sparkService.getSpark(),
                        bookService::findBooksGroupedByAuthor, pageable, Encoders.bean(BookAuthorCount.class))
                .toDF();

        Dataset<Row> authorsDS = DatasetsUtils.renameDatasetColumns(authors, authorRenamedColumns());
        Dataset<Row> booksDS =  DatasetsUtils.renameDatasetColumns(books, bookRenamedColumns());

        Dataset<Row> rowDataset = sparkService.performJoin(booksDS, authorsDS,
                (d1, d2) -> d1.col("author_id").equalTo(d2.col("book_author")));

        Set<UUID> uuids = rowDataset.select(col("author_id"))
                .as(Encoders.STRING())
                .collectAsList().stream()
                .map(UUID::fromString)
                .collect(Collectors.toSet());

        return CompletableFuture.completedFuture(uuids);
    }

    @Override
    public CompletableFuture<Dataset<JoinResult>> finalJoinResult(Endpoints endpoints, Databases databases, Set<UUID> commonIds) {
        Pageable pageable = PageRequest.of(0, 1000);

        Dataset<Row> commonAuthors = DatasetsUtils.createDataset(sparkService.getSpark(),
                        (page) -> authorService.findAllById(commonIds, page), pageable, Encoders.bean(Author.class))
                .withColumnRenamed("id", "author_id");

        Dataset<Row> commonBooks = DatasetsUtils.createDataset(sparkService.getSpark(),
                        (page) -> bookService.findAllByAuthorId(commonIds, page), pageable, Encoders.bean(Book.class))
                .withColumnRenamed("author", "book_author");

        Dataset<Row> rowDataset = sparkService.performJoin(commonBooks, commonAuthors,
                        (authors, books) -> authors.col("author_id").equalTo(books.col("book_author")))
                .withColumnRenamed("author_id", "author")
                .withColumnRenamed("book_author", "bookAuthor");

        Dataset<JoinResult> map = rowDataset.map((MapFunction<Row, JoinResult>) row -> {
            Row authorRow = row.getAs("author");
            Row bookAuthorRow = row.getAs("bookAuthor");
            Row bookIdRow = row.getAs("id");

            String authorUUID = String.valueOf(new UUID(authorRow.getLong(1), authorRow.getLong(0)));
            String bookAuthorUUID = String.valueOf(new UUID(bookAuthorRow.getLong(1), bookAuthorRow.getLong(0)));
            String bookUUID = String.valueOf(new UUID(bookIdRow.getLong(1), bookIdRow.getLong(0)));

            return new JoinResult(authorUUID, bookAuthorUUID, row.getAs("name"), row.getAs("title"), bookUUID);
        }, Encoders.bean(JoinResult.class));

        return CompletableFuture.completedFuture(map);
    }
}
