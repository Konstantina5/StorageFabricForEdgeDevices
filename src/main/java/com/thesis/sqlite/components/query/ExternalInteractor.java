package com.thesis.sqlite.components.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.thesis.sqlite.components.query.base.Attribute;
import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.components.spark.DatasetsUtils;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.dto.join.JoinDto;
import com.thesis.sqlite.dto.request.GetAllResult;
import com.thesis.sqlite.dto.request.JoinRequestBody;
import com.thesis.sqlite.utils.PaginationUtil;
import com.thesis.sqlite.utils.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
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

    public CompletableFuture<Dataset<Row>> getAllPaged(SparkService sparkService, String url, String tableName) {
        CompletableFuture<Dataset<Row>> futures =
                fetchAllPagesFor((page, size) -> fetchPage(url, tableName, page, size))
                        .thenApply(res -> DatasetsUtils.createDataset(sparkService.getSpark(), res)
                                        .toDF());

        return futures;
    }

    private CompletableFuture<List<JsonNode>> fetchAllPagesFor(BiFunction<Integer, Integer,
            CompletableFuture<ResponseEntity<GetAllResult>>> fetchPageFunc) {
        Pageable pageable = PageRequest.of(0, 1000);
        int page = pageable.getPageNumber();
        int size = pageable.getPageSize();

        return fetchPageFunc.apply(0, size)
                .thenCompose(firstPageResult -> {
                    JsonNode body = firstPageResult.getBody().getResult();
                    int totalPages = Optional.ofNullable(firstPageResult.getHeaders().get(PaginationUtil.HeaderNames.PAGE_COUNT))
                            .map(list -> list.get(0))
                            .map(Integer::parseInt)
                            .orElse(0);

                    List<CompletableFuture<JsonNode>> futures = new ArrayList<>();

                    futures.add(CompletableFuture.completedFuture(body));

                    for (int i = 1; i < totalPages; i++) {
                        futures.add(fetchPageFunc.apply(i, size).thenApply(HttpEntity::getBody).thenApply(GetAllResult::getResult));
                    }

                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> futures.stream()
                                    .map(CompletableFuture::join)
                                    .toList());
                });
    }

    private <T> CompletableFuture<ResponseEntity<GetAllResult>> fetchPage(String url, String tableName, int page, int size) {
        URI uri = UriComponentsBuilder.fromUriString(url + "/get_all")
                .queryParam("tableName", tableName)
                .queryParam("page", page)
                .queryParam("size", size)
                .build().toUri();

        return CompletableFuture.supplyAsync(() -> restClient.get()
                        .uri(uri)
                        .retrieve()
                        .toEntity(GetAllResult.class),
                taskExecutor);
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
        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String localView = "CREATE VIEW " + viewName + " AS " + query;

        System.out.println("\nCREATE EXTERNAL LOCAL VIEW [" + url + "]: " + localView);

        return executeQueryCF(url, dropView)
                .thenAccept(__ -> executeQueryCF(url, localView));
    }

    public CompletableFuture<Void> registerLocalView2(String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String localView = "CREATE VIEW " + viewName + " AS " + query;

        System.out.println("\nCREATE LOCAL VIEW: " + localView);


        jdbcTemplate.execute(dropView);
        jdbcTemplate.execute(localView);
        return CompletableFuture.completedFuture(null);
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
        String dropView = "DROP VIEW IF EXISTS " + viewName;

//        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableA + "," + tableB + " WHERE " + joinOn;
        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableB
                + " WHERE id in ('" + ids.stream().map(String::valueOf).collect(Collectors.joining("','")) + "')";

        System.out.println("Filtered view: " + joinView);

        return executeQueryCF(url, dropView)
                .thenAccept(__ -> executeQueryCF(url, joinView));
    }

    public void createTableFromResultSet(QueryResult data, String tableName) {
        String dropTable = "DROP TABLE IF EXISTS " + tableName;

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
        Map<Attribute, List<Object>> propertyMap = joinOn.predicate.stream()
                .map(pre -> {
                    List<Object> values = jdbcTemplate.query(
                            "SELECT " + pre.getLeft().getName() + " FROM " + tableA,
                            (rs, rowNum) -> {
                                Object value = rs.getObject(pre.getLeft().getName());
                                if (value instanceof byte[] bytes) {
                                    ByteBuffer bb = ByteBuffer.wrap(bytes);
                                    return new UUID(bb.getLong(), bb.getLong());
                                }
                                if (value instanceof String str) {
                                    try {
                                        return UUID.fromString(str);
                                    } catch (IllegalArgumentException e) {
                                        return str;
                                    }
                                }
                                return value;
                            }
                    );
                    return new Pair<>(pre.getRight(), values);
                }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        //view name is b_a but should be something like a_b_id_equals_author
        //tableA = b
        //tableB = a
        //joinOn = author_id=author
        String dropView = "DROP VIEW IF EXISTS " + viewName;

        String where = propertyMap.entrySet().stream()
                .map(entry -> entry.getKey().getName() + " in ('" //TODO k: this should change, only handles string currently
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
        String dropView = "DROP VIEW IF EXISTS " + viewName;

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
    public String executeQueryAndPrintResult(String query) {

        //Class.forName(this.jdbcProperties.getDriverName());
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Executing query: ");
        System.out.println(query);
        System.out.println("------------------------------------------------------------------------");

        try (Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                int timeOut = 3600;

//                stmt.setQueryTimeout(timeOut);

                if (query.toLowerCase().contains("select")) {
                    ResultSet rs = stmt.executeQuery(query);
                    //ResultSet rs = stmt.executeQuery("SELECT 1");
                    return UtilsQuery.printResultSet(rs);
                } else {
                    stmt.execute(query);
                    return "";
                }

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
            case Types.BLOB:
                return "BLOB";
            default:
                return "VARCHAR(255)";
        }
    }
}
