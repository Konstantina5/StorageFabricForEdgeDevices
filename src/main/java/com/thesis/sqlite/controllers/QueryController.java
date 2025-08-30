package com.thesis.sqlite.controllers;

import com.thesis.sqlite.algorithm.MetaSpark;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.query.UtilsQuery;
import com.thesis.sqlite.components.query.base.Attribute;
import com.thesis.sqlite.components.query.base.Type;
import com.thesis.sqlite.dto.JoinResult;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.dto.request.JoinRequestBody;
import com.thesis.sqlite.results.Client;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import jakarta.ws.rs.QueryParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.thesis.sqlite.components.query.UtilsQuery.filterQueryByTables;

@RestController
@RequestMapping("api/query")
public class QueryController {
    private final JdbcTemplate jdbcTemplate;
    private final ExternalInteractor externalInteractor;

    @Autowired
    public QueryController(JdbcTemplate jdbcTemplate, ExternalInteractor externalInteractor) {
        this.jdbcTemplate = jdbcTemplate;
        this.externalInteractor = externalInteractor;
    }

    @PostMapping("/execute")
    public ResponseEntity<?> perform(@RequestBody String query) {
        try {
            if(query.toLowerCase().startsWith("select")) {
                try(Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
                    try (Statement statement = connection.createStatement()) {
                        ResultSet execute = statement.executeQuery(query);
                        return Client.Results.ok(UtilsQuery.convertResultSetToList(execute));
                    }

                }
            } else {
                // For ddl or other statements like create, drom, insert, update
                jdbcTemplate.execute(query);
                return Client.Results.ok();
            }
        } catch (Exception e) {
            return Client.Errors.unprocessableEntity();
        }
    }

    @PostMapping("/execute/external_join")
    public CompletableFuture<ResponseEntity<?>> executeExternalJoin(@RequestBody JoinRequestBody joinRequestBody) {
        return Future.allOf(joinRequestBody.getJoins().stream()
                .map(rsj -> externalInteractor.registerJoinView(rsj.getRhs().getBaseUrl(), rsj.getJoinName(),
                        rsj.getLhs().getShortName(), rsj.getRhs().getShortName(), rsj))
                .toList())
                .thenCompose(joinList -> Future.allOf(joinList.stream()
                        .map(pair -> externalInteractor.executeQueryCF(pair.getKey(), "select * from " + pair.getValue(), QueryResult.class)
                                .thenApply(res -> new Pair<>(pair.getValue(), res)))
                        .toList()))
                .thenAccept(pairs -> pairs.stream()
                        .filter(pair -> !pair.getValue().getStatusCode().isError())
                        .forEach(pair -> Optional.ofNullable(pair.getValue())
                                .map(ResponseEntity::getBody)
                                .ifPresent(body -> externalInteractor.createTableFromResultSet(body, pair.getKey().split("_")[1]))))
                .thenApply(__ -> execute(joinRequestBody))
                .thenApply(tt -> UtilsQuery.convertResultSetToList(tt))
                .thenApply(rr -> Client.Results.ok(rr));
    }

    @GetMapping("/metadata")
    public ResponseEntity<List<Attribute>> getMetadata(@QueryParam("table") String table) {
        List<Attribute> attributes = new ArrayList<>();

        try(Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery("select * from " + table + " where 1=0");
                ResultSetMetaData rsMetaData = rs.getMetaData();

                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    String attrName = rsMetaData.getColumnName(i);
                    int columnType = rsMetaData.getColumnType(i);

                    String sqlCountDistinct = String.format("SELECT COUNT(DISTINCT %s) FROM %s", attrName, table);
                    Long count = jdbcTemplate.queryForObject(sqlCountDistinct, Long.class);

                    Type type = columnType == Types.OTHER ? Type.UUID : Type.CHAR; // TODO k: fix

                    Attribute attr = new Attribute(attrName.toLowerCase().replaceAll("\"", ""), count, type);
                    attributes.add(attr);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return Client.Results.ok(attributes);
    }

    public ResultSet execute(JoinRequestBody joinRequestBody) {
        Set<String> collect = joinRequestBody.getJoins().stream()
                .flatMap(join -> Stream.of(join.getLhs().getName(), join.getRhs().getName()))
                .collect(Collectors.toSet());

        String query = filterQueryByTables(joinRequestBody.getOriginalQuery(), collect);

        System.out.println("------------------------------");
        System.out.println("Executing query from controller : \n" + query);
        System.out.println("------------------------------");

        try(Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            try (Statement statement = connection.createStatement()) {
                int timeOut = 3600;

                statement.setQueryTimeout(timeOut);

                ResultSet rs = statement.executeQuery(query);
                UtilsQuery.printResultSet(rs);
                return rs;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
