package com.thesis.sqlite.components;

import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.query.UtilsQuery;
import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.components.query.base.Relation;
import com.thesis.sqlite.dto.QueryResult;
import com.thesis.sqlite.dto.nodes.InfoType;
import com.thesis.sqlite.dto.nodes.NodeInfos;
import com.thesis.sqlite.dto.request.JoinRequestBody;
import com.thesis.sqlite.mappers.ResponsesMapper;
import com.thesis.sqlite.messages.kafka.KafkaMessage;
import com.thesis.sqlite.messages.kafka.NodeAdded;
import com.thesis.sqlite.utils.Future;
import com.thesis.sqlite.utils.Pair;
import com.thesis.sqlite.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.thesis.sqlite.components.query.QueryHandler.getBaseRelationsCF;
import static com.thesis.sqlite.kafka.KafkaTopics.NODE_INFO;

@Component
public class NodesInfoManager {
    private static final String MY_TABLE = System.getenv("DB_NAME");
    private final Map<String, NodeInfos> nodeInfos = new ConcurrentHashMap<>();
    public final Map<String, NodeInfos> tableInfos = new ConcurrentHashMap<>();
    private final ExternalInteractor externalInteractor;
    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public NodesInfoManager(ExternalInteractor externalInteractor, ApplicationEventPublisher eventPublisher) {
        this.externalInteractor = externalInteractor;
        this.eventPublisher = eventPublisher;

//        tableInfos.putIfAbsent("author", new NodeInfos("client", "http://localhost:8080/api", InfoType.AUTHOR,
//                new String[]{"author"}, "author"));
//        tableInfos.putIfAbsent("address", new NodeInfos("client", "http://localhost:8080/api", InfoType.ADDRESS,
//                new String[]{"address"}, "address"));
//        tableInfos.putIfAbsent("book", new NodeInfos("client", "http://localhost:8080/api", InfoType.BOOK,
//                new String[]{"book"}, "book"));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeNode() {
        KafkaMessage<NodeAdded> kafkaMessage = new KafkaMessage<>(NODE_INFO, new NodeAdded(Utils.HOSTNAME,
                new NodeInfos("client", System.getenv("BASE_URL"), InfoType.AUTHOR, new String[]{"author"}, MY_TABLE)));
        //Add id as conf maybe
        eventPublisher.publishEvent(kafkaMessage);
    }

    public CompletableFuture<Void> handleQuery(String sqlQuery) {
        ArrayList<String> joins = UtilsQuery.getJoinStr(sqlQuery);
        HashMap<String, String> aliasMap = UtilsQuery.getAliasMap(sqlQuery);

        boolean b = aliasMap.keySet().stream()
                .allMatch(tableInfos::containsKey);
        if (!b) {
            throw new RuntimeException("Not all tables are present, cannot execute query");
        }


        Map<String, String> collect = aliasMap.keySet().stream()
                .map(s -> new Pair<>(s, Optional.ofNullable(tableInfos.get(s))))
                .filter(entry -> entry.getValue().isPresent()) //TODO k: do not execute query if we do not have infos for a table
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().get()))
                .collect(Collectors.toMap(Pair::getKey, p -> p.getValue().getUrl()));

        return UtilsQuery.registerLocalViewsCF(externalInteractor, collect, sqlQuery, MY_TABLE)
                .thenCompose(tableAnnotations -> getBaseRelationsCF(externalInteractor, collect, aliasMap, true)
                        .thenCompose(baseRelations -> {
                            // the joins in the left they always have the current table,
                            // and if table is not currently present then they are lexicography sorted with the greater being on the left
                            List<Join> joinGraph = UtilsQuery.getJoinGraph(MY_TABLE, baseRelations, joins);

                            List<Join> localJoins = joinGraph.stream()
                                    .filter(join -> join.rhs.name.equals(MY_TABLE) || join.lhs.name.equals(MY_TABLE))
                                    .toList();

                            Map<Relation, List<Join>> external = joinGraph.stream()
                                    .filter(join -> !join.rhs.name.equals(MY_TABLE) && !join.lhs.name.equals(MY_TABLE))
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
                        .thenAccept(__ -> execute(sqlQuery)));
    }

    public void execute(String sqlQuery) {
        String finalQuery = rewriteFromClause(sqlQuery);
        externalInteractor.executeQueryAndPrintResult(finalQuery);
    }

    private static String rewriteFromClause(String sql) {
        String myTableAlias = Optional.ofNullable(UtilsQuery.getAliasMap(sql).get(MY_TABLE)).orElse(MY_TABLE);
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

            boolean referencesTable = condLower.contains(myTableAlias + ".") || condLower.contains(myTableAlias + " ");
            if (referencesTable) {
                // Keep condition if it’s a join (table.column = otherTable.column)
                boolean isJoin = condLower.matches(".*\\b" + myTableAlias + "\\.\\w+\\s*=\\s*\\w+\\.\\w+.*");
                if (isJoin) {
                    newConditions.add(cond.trim());
                }
                // otherwise skip
            } else {
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

    @EventListener
    public void onNodeAdded(NodeAdded nodeAdded) {
        Optional.ofNullable(MY_TABLE)
                .filter(name -> !name.equals(nodeAdded.getNodeInfo().getTableName()))
                .ifPresent(__ -> {
                    nodeInfos.put(nodeAdded.getNodeName(), nodeAdded.getNodeInfo());
                    tableInfos.put(nodeAdded.getNodeInfo().getTableName(), nodeAdded.getNodeInfo());
                });

        System.out.println();
    }

}
