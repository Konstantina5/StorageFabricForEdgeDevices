package com.thesis.sqlite.components;

import com.thesis.sqlite.components.spark.DatasetsUtils;
import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.PaginationUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

@Component
public class ExternalServicesClient {
    private final RestClient restClient;
    private final Executor taskExecutor;
    private final SparkSession sparkSession;

    @Autowired
    public ExternalServicesClient(RestClient restClient, @Qualifier("customTaskExecutor") Executor taskExecutor, SparkSession sparkSession) {
        this.restClient = restClient;
        this.taskExecutor = taskExecutor;
        this.sparkSession = sparkSession;
    }

    public <T> CompletableFuture<List<Dataset<Row>>> getAllPaged(Set<String> urls, Set<UUID> ids, Class<T> clazz,
                                                                 ParameterizedTypeReference<List<T>> requestType) {
        List<CompletableFuture<Dataset<Row>>> futures = urls.stream()
                .map(url -> fetchAllPagesFor((page, size) -> fetchPage(url, page, size, ids, requestType))
                        .thenApply(lists -> lists.stream().flatMap(Collection::stream).toList())
                        .thenApply(idLists -> DatasetsUtils.createDataset(sparkSession, idLists, Encoders.bean(clazz))
                                        .toDF()
//                                .withColumn("source", col(url))
                        ))
                .toList();

        return Future.allOf(futures);
    }

    public CompletableFuture<List<Dataset<Row>>> getAllPaged(Set<String> urls) {

        List<CompletableFuture<Dataset<Row>>> future2 = urls.stream()
                .map(url -> url + "/count")
                .map(url -> fetchAllPagesFor((page, size) -> fetchPage(url, page, size,
                        new ParameterizedTypeReference<List<BookAuthorCount>>() {}))
                        .thenApply(lists -> lists.stream().flatMap(Collection::stream).toList())
                        .thenApply(list -> DatasetsUtils.createDataset(sparkSession, list, Encoders.bean(BookAuthorCount.class))
                                .toDF()))
                .toList();

        return Future.allOf(future2);
    }

    private <T> CompletableFuture<List<T>> fetchAllPagesFor(BiFunction<Integer, Integer,
                                                                       CompletableFuture<ResponseEntity<T>>> fetchPageFunc) {
        Pageable pageable = PageRequest.of(0, 1000);
        int page = pageable.getPageNumber();
        int size = pageable.getPageSize();

        return fetchPageFunc.apply(0, size)
                .thenCompose(firstPageResult -> {
                    T body = firstPageResult.getBody();
                    int totalPages = Optional.ofNullable(firstPageResult.getHeaders().get(PaginationUtil.HeaderNames.PAGE_COUNT))
                            .map(list -> list.get(0))
                            .map(Integer::parseInt)
                            .orElse(0);

                    List<CompletableFuture<T>> futures = new ArrayList<>();

                    futures.add(CompletableFuture.completedFuture(body));

                    for (int i = 1; i < totalPages; i++) {
                        futures.add(fetchPageFunc.apply(i, size).thenApply(HttpEntity::getBody));
                    }

                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> futures.stream()
                                    .map(CompletableFuture::join)
                                    .toList());
                });
    }

    private <T> CompletableFuture<ResponseEntity<List<T>>> fetchPage(String url, int page, int size, ParameterizedTypeReference<List<T>> responseType) {
        URI uri = UriComponentsBuilder.fromUriString(url)
                .queryParam("page", page)
                .queryParam("size", size)
                .build().toUri();

        return CompletableFuture.supplyAsync(() -> restClient.get()
                        .uri(uri)
                        .retrieve()
                        .toEntity(responseType),
                taskExecutor);
    }

    private <T> CompletableFuture<ResponseEntity<List<T>>> fetchPage(String url, int page, int size, Set<UUID> ids, ParameterizedTypeReference<List<T>> responseType) {
        URI uri = UriComponentsBuilder.fromUriString(url)
                .queryParam("page", page)
                .queryParam("size", size)
                .build().toUri();
        return CompletableFuture.supplyAsync(() -> restClient.post()
                        .uri(uri)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(ids)
                        .retrieve()
                        .toEntity(responseType),
                taskExecutor);
    }
}
