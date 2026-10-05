package com.thesis.sqlite.confs;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KsqlDbConfig {

    @Value("${ksqldb.server.url}")
    private String ksqlDbUrl;

    @Bean
    @ConditionalOnProperty(
            value = "streaming",
            havingValue = "true")
    public Client ksqlClient() {
        ClientOptions options = ClientOptions.create()
                .setExecuteQueryMaxResultRows(50000) // Increase to 50k
                .setHost(ksqlDbUrl.replace("http://", "").split(":")[0])
                .setPort(Integer.parseInt(ksqlDbUrl.split(":")[2]));
        return Client.create(options);
    }
}

