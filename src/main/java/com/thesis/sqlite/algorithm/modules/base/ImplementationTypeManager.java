package com.thesis.sqlite.algorithm.modules.base;

import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.request.Databases;
import com.thesis.sqlite.dto.request.Endpoints;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class ImplementationTypeManager {

    public CompletableFuture<List<JoinResult>> performAlgorithm(Endpoints endpoints, Databases databases, Pageable pageable) {
        return findCommonIds(endpoints, databases)
                .thenCompose(commonIds -> finalJoinResult(endpoints, databases, commonIds))
                .thenApply(dataset -> dataset
//                        .limit(pageable)
                        .offset((int) pageable.getOffset()))
                .thenApply(Dataset::collectAsList);
    }

    public CompletableFuture<Dataset<Row>> performAlgorithm(String sqlQuery) {
        throw new RuntimeException();
    }

    public abstract CompletableFuture<Set<UUID>> findCommonIds(Endpoints endpoints, Databases databases);
    public abstract CompletableFuture<Dataset<JoinResult>> finalJoinResult(Endpoints endpoints, Databases databases, Set<UUID> commonIds);
}
