package com.thesis.sqlite.components.query;

import com.thesis.sqlite.components.query.base.Attribute;
import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.dto.join.JoinDto;
import com.thesis.sqlite.dto.request.JoinRequestBody;
import com.thesis.sqlite.utils.Pair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Component
public class ExternalInteractor implements Interactor {
    private final RestClient restClient;
    private final Executor taskExecutor;
    private final JdbcTemplate jdbcTemplate;
    public Map<String, List<String>> registeredViews;
    public Map<String, List<String>> registeredJoinViews;

    public ExternalInteractor(RestClient restClient, @Qualifier("customTaskExecutor") Executor taskExecutor, JdbcTemplate jdbcTemplate) {
        this.restClient = restClient;
        this.taskExecutor = taskExecutor;
        this.jdbcTemplate = jdbcTemplate;
        this.registeredViews = new HashMap<>();
        this.registeredJoinViews = new HashMap<>();
    }

    private void executeQuery(String url, String query) {
        restClient.post()
                .uri(url + "/query/execute")
                .contentType(MediaType.APPLICATION_JSON)
                .body(query);
    }

    public CompletableFuture<ResponseEntity<Void>> executeQueryCF(String url, String query) {
        return CompletableFuture.supplyAsync(() ->
                        restClient.post()
                                .uri(url + "/query/execute")
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(query)
                                .retrieve()
                                .toBodilessEntity(),
                taskExecutor);
    }

    public <T> CompletableFuture<ResponseEntity<T>> executeQueryCF(String url, String query, Class<T> clazz) {
        return CompletableFuture.supplyAsync(() ->
                        restClient.post()
                                .uri(url + "/query/execute")
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(query)
                                .retrieve()
                                .toEntity(clazz),
                taskExecutor);
    }

    public  <T> CompletableFuture<ResponseEntity<T>> executeQueryCF(String url, JoinRequestBody joinRequestBody, Class<T> clazz) {
        return CompletableFuture.supplyAsync(() ->
                        restClient.post()
                                .uri(url + "/query/execute/external_join")
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(joinRequestBody)
                                .retrieve()
                                .toEntity(clazz),
                taskExecutor);
    }

    private <T> CompletableFuture<ResponseEntity<List<T>>> executeGetQueryCF(String url, String table, ParameterizedTypeReference<List<T>> responseType) {
        URI uri = UriComponentsBuilder.fromUriString(url)
                .queryParam("table", table)
                .build().toUri();

        return CompletableFuture.supplyAsync(() ->
                        restClient.get()
                                .uri(uri)
                                .retrieve()
                                .toEntity(responseType),
                taskExecutor);
    }

    @Override
    public CompletableFuture<Void> registerLocalView(String url, String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";
        String localView = "CREATE VIEW " + viewName + " AS " + query;
        return executeQueryCF(url, dropView)
                .thenAccept(__ -> executeQueryCF(url, localView));
    }

    @Override
    public CompletableFuture<List<Attribute>> getAttributes(String url, String tableName, boolean getCount) {
        String query = "SELECT * FROM " + tableName + " where 1=0";
//        String distinctQuery = "SELECT COUNT(DISTINCT " + attrName + ") FROM " + tableName; TODO k: incorporate this into the metadata

        return executeGetQueryCF(url + "/query/metadata", tableName, new ParameterizedTypeReference<List<Attribute>>() {})
                .thenApply(res -> {
                    if(res.getStatusCode().isError()) throw new RuntimeException();
                    return res.getBody();
                });
    }

    @Override
    public CompletableFuture<Void> registerJoinView(String url, String viewName, String tableA, String tableB, String joinOn,
                                                    Collection<UUID> ids) {
        //view name is b_a but should be something like a_b_id_equals_author
        //tableA = b
        //tableB = a
        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";

//        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableA + "," + tableB + " WHERE " + joinOn;
        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableB
                + " WHERE id in ('" + ids.stream().map(String::valueOf).collect(Collectors.joining("','")) + "')";

        System.out.println("Filtered view: " + joinView);

        return executeQueryCF(url, dropView)
                .thenAccept(__ -> executeQueryCF(url, joinView));
    }

    public void createTableFromResultSet(QueryResult data, String tableName) {
//        String dropTable = "DROP TABLE IF EXISTS " + tableName; TODO k: change to DROP Table
        String dropTable = "DROP VIEW IF EXISTS " + tableName + " CASCADE";

        if (data == null) throw new RuntimeException();

        //Use first row to infer columns and types
        Map<String, Integer> columnTypes = data.getColumnTypes();
        List<Map<String, Object>> rows = data.getRows();

        StringBuilder createSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " ("); //TODO k: can be a temp table
        Iterator<Map.Entry<String, Integer>> iter = columnTypes.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Integer> entry = iter.next();
            String columnName = entry.getKey();
            Integer sqlType = entry.getValue();

            createSQL.append(columnName).append(" ").append(mapSqlTypeToDbType(sqlType));
            if (iter.hasNext()) createSQL.append(", ");
        }
        createSQL.append(")");

        System.out.println("Creating table with SQL: " + createSQL);

        try(Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(dropTable);
                stmt.executeUpdate(createSQL.toString());

                //2. Insert data into table
                StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (");
                insertSQL.append(String.join(", ", columnTypes.keySet()));
                insertSQL.append(") VALUES (");
                insertSQL.append("?,".repeat(columnTypes.size()));
                insertSQL.setLength(insertSQL.length() - 1); //remove last comma
                insertSQL.append(")");

                System.out.println(insertSQL);

                try (PreparedStatement ps = connection.prepareStatement(insertSQL.toString())) {
                    for (Map<String, Object> row : rows) {
                        int idx = 1;
                        for (String columnName : columnTypes.keySet()) {
                            Object value = row.get(columnName);
                            int sqlType = columnTypes.get(columnName);
                            if(sqlType == Types.OTHER && value instanceof String) {
                                String str = (String) value;
                                try {
                                    value = UUID.fromString(str);
                                } catch (IllegalArgumentException e) {

                                }
                            }
                            ps.setObject(idx++, value);
                        }
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    //TODO k: should find the external table let's say it would be put in tableB maybe?
    @Override
    public CompletableFuture<Pair<String, String>> registerJoinView(String url, String viewName, String tableA, String tableB, Join joinOn) {
        Map<Attribute, List<UUID>> propertyMap = joinOn.predicate.stream()
                .map(pre -> {
                    List<UUID> uuids = jdbcTemplate.query("SELECT " + pre.getLeft().getName() + " FROM " + tableA,
                            (rs, rowNum) -> UUID.fromString(rs.getString(pre.getLeft().getName())));
                    return new Pair<>(pre.getRight(), uuids);
                }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        //view name is b_a but should be something like a_b_id_equals_author
        //tableA = b
        //tableB = a
        //joinOn = author_id=author
        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";

        String where = propertyMap.entrySet().stream()
                .map(entry -> entry.getKey() + " in ('"
                        + entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("','")) + "')")
                .collect(Collectors.joining(" and "));

        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableB + " WHERE " + where;

        System.out.println("Filtered view: " + joinView);

        return executeQueryCF(url, dropView)
                .thenCompose(__ -> executeQueryCF(url, joinView)
                        .thenApply(res -> new Pair<>(url, viewName)));
    }

    //TODO k: should find the external table let's say it would be put in tableB maybe?
    public CompletableFuture<Pair<String, String>> registerJoinView(String url, String viewName, String tableA, String tableB, JoinDto joinOn) {
        Map<Attribute, List<UUID>> propertyMap = joinOn.getPredicate().stream()
                .map(pre -> {
                    List<UUID> uuids = jdbcTemplate.query("SELECT " + pre.getLeft().getName() + " FROM " + tableA,
                            (rs, rowNum) -> UUID.fromString(rs.getString(pre.getLeft().getName())));
                    return new Pair<>(pre.getRight(), uuids);
                }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        //view name is b_a but should be something like a_b_id_equals_author
        //tableA = b
        //tableB = a
        //joinOn = author_id=author
        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";

        String where = propertyMap.entrySet().stream()
                .map(entry -> entry.getKey() + " in ('"
                        + entry.getValue().stream().map(String::valueOf).collect(Collectors.joining("','")) + "')")
                .collect(Collectors.joining(" and "));

        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableB + " WHERE " + where;

        System.out.println("Filtered view: " + joinView);

        return executeQueryCF(url, dropView)
                .thenCompose(__ -> executeQueryCF(url, joinView)
                        .thenApply(res -> new Pair<>(url, viewName)));
    }

    @Override
    public void registerForeignTable(String systemName, String tableName) {

    }

    @Override
    public boolean registerLocalMaterializedView(String viewName, String query) {
        return false;
    }

    @Override
    public void executeQuery(String query) {

    }

    @Override
    public void executeQueryAndPrintResult(String query) {

        //Class.forName(this.jdbcProperties.getDriverName());
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Executing query: ");
        System.out.println(query);
        System.out.println("------------------------------------------------------------------------");

        try (Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                int timeOut = 3600;

                stmt.setQueryTimeout(timeOut);
                stmt.execute("SET enable_nestloop=off;");

                if (System.getProperties().containsKey("parallel") && System.getProperty("parallel").equals("false"))
                    stmt.executeUpdate("SET max_parallel_workers_per_gather = 0;");

                if (query.toLowerCase(Locale.ROOT).contains("select")) {
                    ResultSet rs = stmt.executeQuery(query);
                    //ResultSet rs = stmt.executeQuery("SELECT 1");
                    UtilsQuery.printResultSet(rs);
                } else
                    stmt.execute(query);

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getQueryCost(String query) {
        return 0;
    }

    @Override
    public void createDummyTable(String tableName, Relation r) {

    }

    @Override
    public void updateStatistics(String tableName, Relation r, boolean analyze) {

    }

    @Override
    public Pair<Connection, ResultSet> executeQueryAndReturnRS(String query) throws SQLException {
        return null;
    }

    @Override
    public String getSystemName() {
        return "";
    }

    @Override
    public HashMap<String, Long> getAttributes(String tableName) {
        return null;
    }

    @Override
    public Long getTableSize(String tableName) {
        return 0L;
    }

    @Override
    public ArrayList<String> getRegisteredViews() {
        return null;
    }

    @Override
    public ArrayList<String> getRegisteredJoinViews() {
        return null;
    }

    @Override
    public ArrayList<String> getRegisteredTables() {
        return null;
    }

    @Override
    public ArrayList<String> getRegisteredForeignTables() {
        return null;
    }

    @Override
    public void cleanUp() {

    }

    private String mapSqlTypeToDbType(int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
                return "INT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.NUMERIC:
            case Types.DECIMAL:
                return "DECIMAL(15,5)";
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return "VARCHAR(255)";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.OTHER:
                return "UUID";
            default:
                return "VARCHAR(255)";
        }
    }
}
