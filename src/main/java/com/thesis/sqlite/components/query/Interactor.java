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

    void registerForeignTable(String systemName, String tableName);

    boolean registerLocalMaterializedView(String viewName, String query);

    void executeQuery(String query);

    void executeQueryAndPrintResult(String query) throws ClassNotFoundException;

    long getQueryCost(String query);

    void createDummyTable(String tableName, Relation r);

    void updateStatistics(String tableName, Relation r, boolean analyze);

    Pair<Connection, ResultSet> executeQueryAndReturnRS(String query) throws SQLException;

    String getSystemName();

    HashMap<String, Long> getAttributes(String tableName);

    Long getTableSize(String tableName);

    //TODO: merge views with joinviews
    ArrayList<String> getRegisteredViews();

    ArrayList<String> getRegisteredJoinViews();

    ArrayList<String> getRegisteredTables();

    ArrayList<String> getRegisteredForeignTables();

    void cleanUp();
}
