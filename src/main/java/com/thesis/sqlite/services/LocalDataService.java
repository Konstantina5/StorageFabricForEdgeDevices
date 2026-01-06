package com.thesis.sqlite.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thesis.sqlite.components.query.UtilsQuery;
import com.thesis.sqlite.components.query.base.Attribute;
import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.components.query.base.Type;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.dto.join.JoinDto;
import com.thesis.sqlite.utils.Pair;
import com.thesis.sqlite.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class LocalDataService {
    private static final Object SQLITE_WRITE_LOCK = new Object();
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public LocalDataService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        jdbcTemplate.execute("PRAGMA journal_mode=WAL;");
        jdbcTemplate.execute("PRAGMA synchronous=NORMAL;");
        jdbcTemplate.execute("PRAGMA busy_timeout=5000;");
    }

    @Transactional
    public void execute(String sql) {
        synchronized (SQLITE_WRITE_LOCK) {
            jdbcTemplate.execute(sql);
        }
    }

    public synchronized String executeQueryAndPrintResult(String query) {

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

    public synchronized List<JsonNode> fetchBatch(String tableName, int limit, int offset) {
        String sql = "SELECT * FROM %s LIMIT ? OFFSET ?".formatted(tableName);

        ObjectMapper mapper = new ObjectMapper();

        return jdbcTemplate.query(sql, rs -> {
            List<JsonNode> result = new ArrayList<>();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            while (rs.next()) {
                ObjectNode node = mapper.createObjectNode();
                for (int i = 1; i <= columnCount; i++) {
                    node.put(meta.getColumnName(i), rs.getString(i));
                }
                result.add(node);
            }
            return result;
        }, limit, offset);
    }

    public synchronized void createTableFromResultSet(QueryResult data, String tableName) {
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

    public synchronized Relation getLocalRelationShips(Map<String, String> tableAnnotations) {
        List<Attribute> attributes = new ArrayList<>();
        try(Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery("select * from " + Utils.TABLE_NAME + " where 1=0");
                ResultSetMetaData rsMetaData = rs.getMetaData();

                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    String attrName = rsMetaData.getColumnName(i);
                    int columnType = rsMetaData.getColumnType(i);

//                    String sqlCountDistinct = String.format("SELECT COUNT(DISTINCT %s) FROM %s", attrName, Utils.TABLE_NAME);
//                    System.out.println("getLocalRelationShips: " + sqlCountDistinct);
//                    Long count = jdbcTemplate.queryForObject(sqlCountDistinct, Long.class);
//
//                    System.out.println("getLocalRelationShips:  count" + count);
                    Long count = 100L;
                    Type type = columnType == Types.OTHER ? Type.UUID : Type.CHAR; // TODO k: fix

                    Attribute attr = new Attribute(attrName.toLowerCase().replaceAll("\"", ""), count, type);
                    attributes.add(attr);
                }
            }

        } catch (SQLException e) {
            System.out.println("getLocalRelationShips: " + e.getMessage());
        }

        Relation r = new Relation(Utils.BASE_URL, Utils.TABLE_NAME, tableAnnotations.get(Utils.TABLE_NAME), 100L, true); //TODO k: get correct size
        r.addAttributes(attributes);
        r.addComposedOf(r);
        return r;
    }

    public synchronized Map<Attribute, List<Object>> getAttributeMap(String table, Join joinOn) {
        return joinOn.predicate.stream()
                .map(pre -> {
                    List<Object> values = jdbcTemplate.query(
                            "SELECT " + pre.getLeft().getName() + " FROM " + table + " LIMIT 100001",
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
    }

    public synchronized Map<Attribute, List<UUID>> getAttributeMap(String table, JoinDto joinOn) {
        return joinOn.getPredicate().stream()
                .map(pre -> {
                    List<UUID> uuids = jdbcTemplate.query("SELECT " + pre.getLeft().getName() + " FROM " + table,
                            (rs, rowNum) -> UUID.fromString(rs.getString(pre.getLeft().getName())));
                    return new Pair<>(pre.getRight(), uuids);
                }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
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
