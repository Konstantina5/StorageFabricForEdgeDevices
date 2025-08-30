package com.thesis.sqlite.controllers;

import com.thesis.sqlite.algorithm.MetaSpark;
import com.thesis.sqlite.dto.JoinResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("api/algorithm")
public class AlgoController {
    private final MetaSpark metaSpark;

    @Autowired
    public AlgoController(MetaSpark metaSpark) {
        this.metaSpark = metaSpark;
    }

    @PostMapping
    //maybe do not use a pageable here, just store the result to a file and not return to the user?
    public CompletableFuture<List<JoinResult>> perform(@RequestBody(required = false)com.thesis.sqlite.dto.request.RequestBody requestBody,
                                                       Pageable pageable) {
        return metaSpark.implementMetaX(requestBody.endpoints(), requestBody.databases(), pageable);
    }
}
