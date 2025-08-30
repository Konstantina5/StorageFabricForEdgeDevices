package com.thesis.sqlite.components.query;

import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.utils.Future;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class QueryHandler {
    public static CompletableFuture<List<Relation>> getBaseRelationsCF(ExternalInteractor externalInteractor,
                                                                       Map<String, String> urls,
                                                                       Map<String, String> tableAnnotations,
                                                                       boolean getRealStats) {
        return Future.allOf(urls.keySet().stream()
                .map(tableName -> UtilsQuery.getTableSizeCF(externalInteractor, tableName, getRealStats)
                        .thenCombine(externalInteractor.getAttributes(urls.get(tableName), tableName, getRealStats), (size, attr) -> {
                            Relation r = new Relation(urls.get(tableName), tableName, tableAnnotations.get(tableName), size, true);
                            r.addAttributes(attr);
                            r.addComposedOf(r);
                            return r;
                        }))
                .toList());
    }
}
