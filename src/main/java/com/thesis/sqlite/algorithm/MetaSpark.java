package com.thesis.sqlite.algorithm;

import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.request.Databases;
import com.thesis.sqlite.dto.request.Endpoints;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class MetaSpark {
    private final ImplementationTypeManager implementation;

    @Autowired
    private MetaSpark(ImplementationTypeManager implementation) {
        this.implementation = implementation;
    }


    public CompletableFuture<List<JoinResult>> implementMetaX(Endpoints endpoints, Databases databases, Pageable pageable) {
        return implementation.performAlgorithm(endpoints, databases, pageable);
    }

}
