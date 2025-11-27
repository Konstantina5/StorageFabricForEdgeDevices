package com.thesis.sqlite.algorithm;

import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import org.springframework.beans.factory.annotation.Autowired;
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

    public CompletableFuture<List<String>> implementMetaX(String query) {
        return implementation.performAlgorithm(query)
                .thenApply(dataset -> dataset.toJSON().collectAsList());
    }

}
