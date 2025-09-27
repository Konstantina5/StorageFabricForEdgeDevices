package com.thesis.sqlite.algorithm;

import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.query.UtilsQuery;
import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.dto.request.JoinRequestBody;
import com.thesis.sqlite.mappers.ResponsesMapper;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import com.thesis.sqlite.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.thesis.sqlite.components.query.QueryHandler.getBaseRelationsCF;

@Component
public class Views {
    private NodesInfoManager infoManager;
    private final ExternalInteractor externalInteractor;

    @Autowired
    public Views(NodesInfoManager infoManager, ExternalInteractor externalInteractor) {
        this.infoManager = infoManager;
        this.externalInteractor = externalInteractor;
    }

    public CompletableFuture<String> handleQuery(String sqlQuery) {
        ArrayList<String> joins = UtilsQuery.getJoinStr(sqlQuery);
        HashMap<String, String> aliasMap = UtilsQuery.getAliasMap(sqlQuery);

        boolean b = aliasMap.keySet().stream()
//                .filter(key -> !key.equals(Utils.TABLE_NAME)) TODO k: uncomment when QueryHandler is ready
                .allMatch(infoManager.getTableInfos()::containsKey);
        if (!b) {
            throw new RuntimeException("Not all tables are present, cannot execute query");
        }


        Map<String, String> collect = aliasMap.keySet().stream()
                .map(s -> new Pair<>(s, Optional.ofNullable(infoManager.getTableInfos().get(s))))
                .filter(entry -> entry.getValue().isPresent())
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().get()))
                .collect(Collectors.toMap(Pair::getKey, p -> p.getValue().getUrl()));

        return UtilsQuery.registerLocalViewsCF(externalInteractor, collect, sqlQuery, Utils.TABLE_NAME)
                .thenCompose(tableAnnotations -> getBaseRelationsCF(externalInteractor, collect, aliasMap, true)
                        .thenCompose(baseRelations -> {
                            // the joins in the left they always have the current table,
                            // and if table is not currently present then they are lexicography sorted with the greater being on the left
                            List<Join> joinGraph = UtilsQuery.getJoinGraph(Utils.TABLE_NAME, baseRelations, joins);

                            List<Join> localJoins = joinGraph.stream()
                                    .filter(join -> join.rhs.name.equals(Utils.TABLE_NAME) || join.lhs.name.equals(Utils.TABLE_NAME))
                                    .toList();

                            Map<Relation, List<Join>> external = joinGraph.stream()
                                    .filter(join -> !join.rhs.name.equals(Utils.TABLE_NAME) && !join.lhs.name.equals(Utils.TABLE_NAME))
                                    .collect(Collectors.groupingBy(j -> j.lhs)); //TODO k: maybe i can group based on the url

                            return Future.allOf(localJoins.stream()
                                            .map(rsj -> externalInteractor.registerJoinView(rsj.rhs.baseUrl, rsj.getJoinName(),
                                                            rsj.lhs.shortName, rsj.rhs.shortName, rsj)
                                                    .thenCompose(pair -> externalInteractor.executeQueryCF(pair.getKey(),
                                                                    "SELECT * FROM " + pair.getValue(), QueryResult.class)
                                                            .thenApply(res -> new Pair<>(pair.getValue(), res)))
                                                    .thenAccept(pair -> Optional.ofNullable(pair.getValue())
                                                            .map(ResponseEntity::getBody)
                                                            .ifPresent(body -> externalInteractor.createTableFromResultSet(body, pair.getKey().split("_")[1]))))
                                            .toList())
                                    .thenApply(__ -> external);
                        }).thenCompose(externalJoins -> Future.allOf(externalJoins.entrySet().stream()
                                .map(entry -> {
                                    JoinRequestBody joinRequestBody = new JoinRequestBody(sqlQuery, ResponsesMapper.convert(entry.getValue()));
                                    return externalInteractor.executeQueryCF(entry.getKey().baseUrl, joinRequestBody, QueryResult.class)
                                            .thenApply(res -> new Pair<>(entry.getKey(), res))
                                            .thenAccept(pair -> Optional.ofNullable(pair.getValue())
                                                    .map(ResponseEntity::getBody)
                                                    .ifPresent(body -> externalInteractor.createTableFromResultSet(body, pair.getKey().name)));
                                }).toList()))
                        .thenApply(__ -> execute(sqlQuery)));
    }

    public String execute(String sqlQuery) {
        String finalQuery = rewriteFromClause(sqlQuery);
        return externalInteractor.executeQueryAndPrintResult(finalQuery);
    }

    private static String rewriteFromClause(String sql) {
        String myTableAlias = Optional.ofNullable(UtilsQuery.getAliasMap(sql).get(Utils.TABLE_NAME)).orElse(Utils.TABLE_NAME);
        String lower = sql.toLowerCase();

        int fromIndex = lower.indexOf("from");
        int whereIndex = lower.indexOf("where");

        if(fromIndex == -1 || whereIndex == -1 || fromIndex > whereIndex) {
            throw new IllegalArgumentException("Invalid sql query: " + sql);
        }

        // 1. Handle FROM clause aliases
        String fromClause = sql.substring(fromIndex + 4, whereIndex).trim(); //remove from

        String regex = "\\b(\\w+)\\s+(?:as\\s+)?(\\w+)\\b";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(fromClause);

        List<String> aliases = new ArrayList<>();
        while (matcher.find()) {
            String alias = matcher.group(2); // the alias name
            aliases.add(alias);
        }

        //Join aliases back together
        String newFromClause = String.join(", ", aliases);

        // Replaces the old FROM clause in the original SQL with the new one
        String beforeFrom = sql.substring(0, fromIndex + 4); // 'select ... from'
        String afterFrom = " " + newFromClause + " ";


        // Handle WHERE clause cleanup
        // since the filtering of local table is made through the view is not needed in the query
        // Extract everything after WHERE

        // Find the next clause: GROUP BY / ORDER BY / LIMIT
        int groupByIndex = lower.indexOf("group by", whereIndex);
        int orderByIndex = lower.indexOf("order by", whereIndex);
        int limitIndex = lower.indexOf("limit", whereIndex);

        int nextClauseIndex = -1;
        for (int idx : new int[]{groupByIndex, orderByIndex, limitIndex}) {
            if (idx != -1 && (nextClauseIndex == -1 || idx < nextClauseIndex)) {
                nextClauseIndex = idx;
            }
        }

        // Extract WHERE conditions and the rest of the query
        String whereConditionsPart;
        String restOfQuery;
        if (nextClauseIndex != -1) {
            whereConditionsPart = sql.substring(whereIndex + 5, nextClauseIndex).trim(); // after "WHERE"
            restOfQuery = sql.substring(nextClauseIndex); // GROUP BY / ORDER BY / LIMIT and beyond
        } else {
            whereConditionsPart = sql.substring(whereIndex + 5).trim();
            restOfQuery = "";
        }

        // Split conditions by AND (preserve parentheses)
        String[] conditions = whereConditionsPart.split("(?i)\\s+and\\s+");
        List<String> newConditions = new ArrayList<>();

        for (String cond : conditions) {
            String condLower = cond.toLowerCase().trim();

            // Keep only join-style conditions: alias.col = otherAlias.col
            boolean isJoin = condLower.matches(".*\\b\\w+\\.\\w+\\s*=\\s*\\w+\\.\\w+.*");
            if (isJoin) {
                newConditions.add(cond.trim());
            }
        }

        // Rebuild WHERE clause
        String newWhereClause = "";
        if (!newConditions.isEmpty()) {
            newWhereClause = "WHERE " + String.join(" AND ", newConditions);
        }

        // Rebuild final query
        return beforeFrom + afterFrom + newWhereClause + " " + restOfQuery;


//        return beforeFrom + afterFrom + "where " + newWhereClause + " " + restOfQuery;
    }
}
