package com.thesis.sqlite.controllers;

import com.thesis.sqlite.algorithm.MetaSpark;
import com.thesis.sqlite.algorithm.Views;
import com.thesis.sqlite.dto.ResultWithTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("api/algorithm")
public class AlgoController {
    private final MetaSpark metaSpark;
    private final Views views;

    @Autowired
    public AlgoController(MetaSpark metaSpark, Views views) {
        this.metaSpark = metaSpark;
        this.views = views;
    }

    @PostMapping("/spark")
    public CompletableFuture<ResultWithTime> perform(@RequestBody String query) {
        Instant start = Instant.now();
        return metaSpark.implementMetaX(query)
                .thenApply(res -> {
                    Instant end = Instant.now();
                    return new ResultWithTime(end.toEpochMilli() - start.toEpochMilli(), res);
                });
    }

    @PostMapping("/views_query")
    public CompletableFuture<ResultWithTime> performViews(@RequestBody String query) {
        Instant start = Instant.now();
        return views.handleQuery(query)
                .thenApply(res -> {
                    Instant end = Instant.now();
                    return new ResultWithTime(end.toEpochMilli() - start.toEpochMilli(), Collections.singletonList(res));
                });
    }
}
