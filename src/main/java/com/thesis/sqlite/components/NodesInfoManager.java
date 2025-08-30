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
    private static final String MY_TABLE = "author"; //TODO k : add it as conf
    private final Map<String, NodeInfos> nodeInfos = new ConcurrentHashMap<>();
    private final Map<String, NodeInfos> tableInfos = new ConcurrentHashMap<>();
    private final ExternalInteractor externalInteractor;
    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public NodesInfoManager(ExternalInteractor externalInteractor, ApplicationEventPublisher eventPublisher) {
        this.externalInteractor = externalInteractor;
        this.eventPublisher = eventPublisher;
        tableInfos.putIfAbsent("author", new NodeInfos("client", "http://localhost:8080/api", InfoType.AUTHOR,
                new String[]{"author"}));
        tableInfos.putIfAbsent("address", new NodeInfos("client", "http://localhost:8080/api", InfoType.ADDRESS,
                new String[]{"address"}));
        tableInfos.putIfAbsent("book", new NodeInfos("client", "http://localhost:8080/api", InfoType.BOOK,
                new String[]{"book"}));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeNode() {
        KafkaMessage<NodeAdded> kafkaMessage = new KafkaMessage<>(NODE_INFO, new NodeAdded(Utils.HOSTNAME,
                new NodeInfos("client", "http://localhost:8080/api", InfoType.AUTHOR, new String[]{"author"}))); //Add id as conf maybe
        eventPublisher.publishEvent(kafkaMessage);
    }

    public CompletableFuture<Void> handleQuery(String sqlQuery) {
        ArrayList<String> joins = UtilsQuery.getJoinStr(sqlQuery);
        HashMap<String, String> aliasMap = UtilsQuery.getAliasMap(sqlQuery);

        Map<String, String> collect = aliasMap.keySet().stream()
                .map(s -> new Pair<>(s, Optional.ofNullable(tableInfos.get(s))))
                .filter(entry -> entry.getValue().isPresent()) //TODO k: do not execute query if we do not have infos for a table
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().get()))
                .collect(Collectors.toMap(Pair::getKey, p -> p.getValue().getUrl()));

        return UtilsQuery.registerLocalViewsCF(externalInteractor, collect, sqlQuery)
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
        String lower = sql.toLowerCase();

        int fromIndex = lower.indexOf("from");
        int whereIndex = lower.indexOf("where");

        if(fromIndex == -1 || whereIndex == -1 || fromIndex > whereIndex) {
            throw new IllegalArgumentException("Invalid sql query: " + sql);
        }

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
        String afterWhere = sql.substring(whereIndex); // 'where ...'

        return beforeFrom + " " + newFromClause + " " + afterWhere;
    }

    @EventListener
    public void onNodeAdded(NodeAdded nodeAdded) {
        nodeInfos.put(nodeAdded.getNodeName(), nodeAdded.getNodeInfo());
    }

}
