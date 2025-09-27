package com.thesis.sqlite.components.query;

import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.components.query.traversal.XNode;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class UtilsQuery {

    public static String printResultSet(ResultSet rs) throws SQLException {

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        StringBuilder out = new StringBuilder();

        boolean isEmpty = false;
        try {
            if (!rs.isBeforeFirst())
                isEmpty = true;
        } catch (SQLFeatureNotSupportedException e) {}

        if (isEmpty) {
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Empty ResultSet");
            System.out.println("------------------------------------------------------------------------");
        } else {
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Query Result:");
            System.out.println("------------------------------------------------------------------------");

            // Print column names
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) out.append("|");
                out.append(rsmd.getColumnName(i));
            }
            out.append("\n");

            while (rs.next()) {
                for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
                    if (i != 1) out.append("|");
                    out.append(rs.getString(i));
                }
                out.append("\n");
            }
            System.out.println(out);
            System.out.println("------------------------------------------------------------------------");
            return out.toString();
        }
        return out.toString();
    }

    //TODO k
    public static CompletableFuture<Long> getTableSizeCF(Interactor interactor, String tableName, boolean getReal) {
        long tableSize = 100L;

        String cntQuery = "SELECT COUNT(*) FROM " + tableName;

        return CompletableFuture.completedFuture(tableSize);
    }


    public static List<Join> getJoinGraph(String currStoredTable, List<Relation> baseRelations, List<String> joins) {
        List<Join> joinGraph = new ArrayList<>();
        for (String jStr : joins) {

            Join j = new Join();

            String[] keys = jStr.split("=");
            String[] lhs = keys[0].split("\\.");
            String[] rhs = keys[1].split("\\.");
            String lhsTable = lhs[0];
            String lhsAttr = lhs[1].replaceAll("\"", "");
            String rhsTable = rhs[0];
            String rhsAttr = rhs[1].replaceAll("\"", "");

            Relation r = getRelByShortName(baseRelations, lhsTable);
            Relation s = getRelByShortName(baseRelations, rhsTable);

            if ( (r.hasAttribute(lhsAttr) && s.hasAttribute(rhsAttr))
                || (r.hasAttribute(rhsAttr) && s.hasAttribute(lhsAttr))) {
                if(r.name.equals(currStoredTable)) {
                    j.addPredicate(r.getAttribute(lhsAttr), s.getAttribute(rhsAttr));
                    j.lhs = r;
                    j.rhs = s;
                } else {
                    if(r.shortName.compareTo(s.name) >= 0) {
                        j.addPredicate(r.getAttribute(lhsAttr), s.getAttribute(rhsAttr));
                        j.lhs = r;
                        j.rhs = s;
                    } else {
                        j.addPredicate(s.getAttribute(rhsAttr), r.getAttribute(lhsAttr));
                        j.lhs = s;
                        j.rhs = r;
                    }
                }
            } else {
                System.out.println(r.name + " and " + s.name + " do not join!");
            }
            joinGraph.add(j);
        }
        return joinGraph;
    }

    public static Relation getRelByShortName(List<Relation> relations, String shortName) {
        for (Relation r : relations) {
            if (r.shortName.equals(shortName))
                return r;
        }
        return null;
    }

    public static Properties loadPropsFromFile(String propertiesFile) {

        Properties prop = new Properties();

        try (InputStream input = new FileInputStream(propertiesFile)) {

            prop.load(input);


        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    //TODO k: maybe in the view add the common ids only on this server that tries to calculate the results
    public static CompletableFuture<List<Pair<String, Interactor>>> registerLocalViewsCF(ExternalInteractor interactor,
                                                                                         Map<String, String> tableDist,
                                                                                         String query, String myTable) {
        ArrayList<Pair<String, Interactor>> tableAnnotations = new ArrayList<>();
        HashMap<String, String> aliasMap = getAliasMap(query); //(author, a)

        return Future.allOf(aliasMap.entrySet().stream()
                        .map(entry -> { //(author, a)
                            String shortTableName = entry.getValue();
                            String fullTableName = entry.getKey();
                            String url = tableDist.get(entry.getKey());

                            String localView = getLocalView(fullTableName, aliasMap.get(fullTableName), query);
                            tableAnnotations.add(new Pair<>(aliasMap.get(shortTableName), interactor));
                            return Optional.of(myTable)
                                    .filter(table -> table.equals(fullTableName))
                                    .map(table -> interactor.registerLocalView2(aliasMap.get(fullTableName), localView))
                                    .orElseGet(() -> interactor.registerLocalView(url, aliasMap.get(fullTableName), localView));
//                            return interactor.registerLocalView(url, aliasMap.get(fullTableName), localView);
                        }).toList())
                .thenApply(__ -> tableAnnotations);
    }

    //assumes that all tables are available on system sysName
    public static CompletableFuture<Void> updateRealCardinalitiesCF(String currStoredTable, Interactor interactor,
                                                                    String url, Map<String, String> tableDist,
                                                                    String query, Relation r, List<Join> joinGraph,
                                                                    Collection<UUID> currentIds) {

        try {
            ArrayList<String> shortNames = new ArrayList<>(Arrays.asList(r.shortName.split("_")));
            shortNames.remove(shortNames.get(0));

            for (String shortName : shortNames) {
                Relation rightRel = r.getComposedRelationByShortName(shortName);
                String leftRelStr;
                if (shortNames.indexOf(shortName) == shortNames.size() - 1) {
                    leftRelStr = r.shortName.substring(0, r.shortName.lastIndexOf("_"));
                } else
                    leftRelStr = r.shortName.substring(0, r.shortName.indexOf("_" + rightRel.shortName + "_"));


                Relation leftRel = r.getComposedRelationByShortName(leftRelStr);
                Join rsj = leftRel.getJoin(rightRel, joinGraph);

                interactor.registerJoinView(rsj.rhs.baseUrl, rsj.getJoinName(), rsj.lhs.shortName, rsj.rhs.shortName, rsj);
                Relation joinRel = r.getComposedRelationByShortName(rsj.getJoinName());

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(null);
    }

    public static HashMap<String, String> getAliasMap(String query) {
        String aliases = subStrBetween(query, "from", "where");
        String[] aliasArr = aliases.split(",");
        HashMap<String, String> aliasMap = new HashMap<>();
        for (String alias : aliasArr) {
            String[] splitted = alias.split(" as ");

            String tableName = sanitize(splitted[0]);
            String shortAlias = sanitize(splitted[1]);

            aliasMap.put(tableName, shortAlias);
        }
        return aliasMap;

    }

    public static HashMap<String, String> getInversedAliasMap(HashMap<String, String> aliasMap) {
        HashMap<String, String> inversedAliasMap = new HashMap<>();
        aliasMap.forEach((key, value) -> inversedAliasMap.put(value, key));
        return inversedAliasMap;
    }

    public static ArrayList<String> getJoinStr(String query) {


        ArrayList<String> joinStr = new ArrayList<>();
        try {
            String selPredStr = subStrBetween(query, "where", "group");
            //System.out.println(selPredStr);
            String[] selPreds = selPredStr.split(" and ");

            for (String selPred : selPreds) {
                String[] selPredOps = selPred.split(" = | < | > | <= | >= | LIKE ");
                String lhs = selPredOps[0];
                String rhs = selPredOps[1];
                if (lhs.contains(".") && rhs.contains(".")) {
                    joinStr.add(sanitize(selPred));

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return joinStr;
    }

    public static String getLocalView(String tableName, String alias, String query) {
        StringBuilder projection = new StringBuilder();
        StringBuilder selection = new StringBuilder();

        String projAttrs = subStrBetween(query, "select", "from");
        String[] projAttrArr = projAttrs.split(" ");
        String delimiter1 = "";
        for (String projAttr : projAttrArr) {
            if (projAttr.contains(".") && projAttr.split("\\.")[0].equals(alias)) {
                String attr = sanitize(projAttr.split("\\.")[1]);
                if (!projection.toString().contains(attr)) {
                    projection.append(delimiter1).append(attr);
                    delimiter1 = ",";
                }
            }
        }

        String selPredStr = subStrBetween(query, "where", "group");
        String[] selPreds = selPredStr.split(" and ");
        String delimiter2 = "";
        for (String selPred : selPreds) {
            String[] selPredOps = selPred.split(" = | < | > | <= | >= | LIKE ");
            String lhs = selPredOps[0];
            String rhs = selPredOps[1];
            if (!(lhs.contains(".") && rhs.contains("."))) {

                if (lhs.split("\\.")[0].equals(alias) || rhs.split("\\.")[0].equals(alias)) {
                    selection.append(delimiter2).append(sanitize(selPred.replace(alias + ".", "")));
                    delimiter2 = " AND ";
                }

            } else {
                String attr = sanitize(lhs.split("\\.")[1]);
                if (lhs.split("\\.")[0].equals(alias) && !projection.toString().contains(attr)) {
                    projection.append(delimiter1).append(attr);
                    delimiter1 = ",";
                }
                attr = sanitize(rhs.split("\\.")[1]);
                if (rhs.split("\\.")[0].equals(alias) && !projection.toString().contains(attr)) {
                    projection.append(delimiter1).append(attr);
                    delimiter1 = ",";
                }
            }
        }

        if (!selection.isEmpty()) {
            selection.insert(0, " WHERE ");
        }

        String localView = "SELECT " + projection + " FROM " + tableName + selection;
        return localView;

    }

    public static String subStrBetween(String str, String open, String close) {
        if (str == null || open == null || close == null) {
            return null;
        }
        int start = str.indexOf(open);
        if (start != -1) {
            int end = str.indexOf(close, start + open.length());
            if (end != -1) {
                return str.substring(start + open.length(), end);
            }
        }
        return null;
    }

    public static String sanitize(String str) {
        if (!str.contains("'"))
            return str.replace(" ", "").replace("\n", "").replace(",", "");
        else
            return str.replace("\n", "").replace(",", "");
    }

    public static String getResourceAsString(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (InputStreamReader isr = new InputStreamReader(is);
             BufferedReader br = new BufferedReader(isr);) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            is.close();
        }
        return sb.toString();
    }

//    public static void printCalcitePlan(String header, RelNode relTree) {
//        try {
//            StringWriter sw = new StringWriter();
//
//            sw.append(header).append(":").append("\n");
//
//            RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);
//
//            relTree.explain(relWriter);
//
//            System.out.println(sw);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public static XNode constructOperatorTree(Relation r, ArrayList<Join> joinGraph) {
        ArrayList<String> shortNames = new ArrayList<>(Arrays.asList(r.shortName.split("_")));

        XNode nodePlan = new XNode(r.getComposedRelationByShortName(shortNames.get(0)));
        shortNames.remove(shortNames.get(0));

        for (String shortName : shortNames) {

            Relation rightRel = r.getComposedRelationByShortName(shortName);
            String leftRelStr;
            if (shortNames.indexOf(shortName) == shortNames.size() - 1) {
                leftRelStr = r.shortName.substring(0, r.shortName.lastIndexOf("_"));
            } else
                leftRelStr = r.shortName.substring(0, r.shortName.indexOf("_" + rightRel.shortName + "_"));

            Relation leftRel = r.getComposedRelationByShortName(leftRelStr);

            //System.out.println("Iteration  joining " + leftRelStr + " and " + rightRel);
            XNode<Relation> rightNode = new XNode<>(rightRel);
            Join rsj = leftRel.getJoin(rightRel, joinGraph);
//            rsj.onDbms = r.getComposedRelationByShortName(rsj.getJoinName()).dbms;
            XNode<Join> join = new XNode<>(rsj);
            join.left = nodePlan;
            join.right = rightNode;
            nodePlan = join;

        }
        return nodePlan;
    }

    public static void printJoinTree(XNode node) {

        if (node != null) {
            printJoinTree(node.left);
            printJoinTree(node.right);
            if (node.data instanceof Join) {
                System.out.println(node.data);

            }
        }
    }

    public static void writeJoinTreeToFile(XNode node, File f) throws IOException {

        if (node != null) {

            writeJoinTreeToFile(node.left, f);
            writeJoinTreeToFile(node.right, f);
            if (node.data instanceof Join) {
                FileUtils.writeStringToFile(f, node.data.toString() + "\n", StandardCharsets.UTF_8, true);

            }

        }
    }

    public static QueryResult convertResultSetToList(ResultSet rs) {
        List<Map<String, Object>> rows = new ArrayList<>();

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            //Store column types for all columns
            Map<String, Integer> columnTypes = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                int columnType = metaData.getColumnType(i);
                columnTypes.put(columnName, columnType);
            }

            //Process rows
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object columnValue = rs.getObject(i);
                    row.put(columnName, columnValue);
                }
                rows.add(row);
            }

            return new QueryResult(columnTypes, rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String filterQueryByTables(String sql, Collection<String> keepTables) {
        String lower = sql.toLowerCase();


        int fromIdx = lower.indexOf("from");
        int whereIdx = lower.indexOf("where");
        int groupIdx = lower.indexOf("group by");
        int orderIdx = lower.indexOf("order by");
        int limitIdx = lower.indexOf("limit");

        //Extract parts
        String selectPart = sql.substring(0, fromIdx).trim();
        String fromPart = sql.substring(fromIdx, whereIdx).trim();
        String wherePart = whereIdx != -1 && groupIdx != -1 ? sql.substring(whereIdx, groupIdx).trim() : "";
        String groupPart = groupIdx != -1 && orderIdx != -1 ? sql.substring(groupIdx, orderIdx).trim() : "";
        String orderPart = orderIdx != -1 && limitIdx != -1 ? sql.substring(orderIdx, limitIdx).trim() : "";
        String limitPart = limitIdx != -1 ? sql.substring(limitIdx).trim() : "";

        // STEP 1: Parse FROM to get alias -> table mapping
        Map<String, String> aliasToTable = new LinkedHashMap<>();
        Pattern aliasPattern = Pattern.compile("(\\w+)\\s+(?:as\\s+)?(\\w+)");

        Matcher matcher = aliasPattern.matcher(fromPart);

        while (matcher.find()) {
            String table = matcher.group(1);
            String alias = matcher.group(2);
            aliasToTable.put(alias, table);
        }

        //STEP 2: Determine aliases to keep
        Set<String> allowedAliases = aliasToTable.entrySet().stream()
                .filter(e -> keepTables.contains(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // STEP 3: Filter SELECT, WHERE, GROUP BY, ORDER By clauses
        String filteredSelect = filterClause(selectPart, allowedAliases);
        String filteredWhere = filterClause(wherePart, allowedAliases);
        String filteredGroup = filterClause(groupPart, allowedAliases);
        String filteredOrder = filterClause(orderPart, allowedAliases);

        // STEP 4: Build filtered FROM clause
        String filteredFrom = "from " + aliasToTable.keySet().stream()
                .filter(allowedAliases::contains) // only alias - can use full table name if needed
                .collect(Collectors.joining(", "));

        // STEP 5: Rebuild query
        return String.join(" ",
                filteredSelect,
                filteredFrom,
                filteredWhere,
                filteredGroup,
                filteredOrder,
                limitPart
        ).replaceAll("\\s+", " ").trim();
    }

    private static String filterClause(String clause, Set<String> allowedAliases) {
        if(clause == null || clause.isBlank()) return "";

        // Split by comma or AND (not 100% safe for sql)
        String[] parts = clause.split("(?i)(,|\\band\\b)");

        List<String> kept = new ArrayList<>();
        for(String part : parts) {
            String trimmed = part.trim();
            for(String alias : allowedAliases) {
                if (trimmed.matches(".*\\b" + alias + "\\..*")) {
                    kept.add(trimmed);
                    break;
                }
            }
        }

        if(kept.isEmpty()) return "";

        String keyword = clause.trim().toLowerCase().startsWith("select") ? "select" :
                clause.trim().toLowerCase().startsWith("where") ? "where" :
                        clause.trim().toLowerCase().startsWith("group") ? "group by" :
                                clause.trim().toLowerCase().startsWith("order") ? "order by" : "";

        return keyword + " " + String.join(keyword.equals("where") ? " and " : ", ", kept);

    }
}
