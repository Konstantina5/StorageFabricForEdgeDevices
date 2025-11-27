package com.thesis.sqlite.algorithm.modules.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.concurrent.CompletableFuture;

public abstract class ImplementationTypeManager {

    public CompletableFuture<Dataset<Row>> performAlgorithm(String sqlQuery) {
        throw new RuntimeException();
    }
}
