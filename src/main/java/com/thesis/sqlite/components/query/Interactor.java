package com.thesis.sqlite.components.query;

import com.thesis.sqlite.components.query.base.Attribute;
import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.utils.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public interface Interactor {
    CompletableFuture<Void> registerLocalView(String url, String viewName, String query);
    CompletableFuture<List<Attribute>> getAttributes(String url, String tableName, boolean getCount);

    CompletableFuture<Void> registerJoinView(String url, String viewName, String tableA, String tableB, String joinOn,
                                             Collection<UUID> ids);

    CompletableFuture<Pair<String, String>> registerJoinView(String url, String viewName, String tableA, String tableB, Join joinOn);

    String executeQueryAndPrintResult(String query) throws ClassNotFoundException;

    Long getTableSize(String tableName);
}
