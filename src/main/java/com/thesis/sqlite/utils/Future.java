package com.thesis.sqlite.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Future {

    public static <T> CompletableFuture<List<T>> allOf(CompletableFuture<T>... futures) {
        return CompletableFuture.allOf(futures)
                .thenApply(__ -> Stream.of(futures)
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    public static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
        return Future.allOf(futures.toArray(new CompletableFuture[0]));
    }
}
