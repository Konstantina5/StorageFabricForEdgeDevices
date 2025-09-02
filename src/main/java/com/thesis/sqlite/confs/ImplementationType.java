package com.thesis.sqlite.confs;

import com.thesis.sqlite.algorithm.modules.Advanced;
import com.thesis.sqlite.algorithm.modules.Database;
import com.thesis.sqlite.algorithm.modules.Endpoints;
import com.thesis.sqlite.algorithm.modules.Local;
import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.ExternalServicesClient;
import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.spark.SparkService;
import com.thesis.sqlite.services.AuthorService;
import com.thesis.sqlite.services.BookService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ImplementationType {

    @Bean
    @ConditionalOnProperty(
            value = "implementation.type",
            havingValue = "database")
    public ImplementationTypeManager getDatabaseType(SparkService sparkService) {
        return new Database(sparkService);

    }

    @Bean
    @ConditionalOnProperty(
            value = "implementation.type",
            havingValue = "endpoint")
    public ImplementationTypeManager getEndpointType(ExternalServicesClient externalServicesClient, SparkService sparkService) {
        return new Endpoints(externalServicesClient, sparkService);
    }

    @Bean
    @ConditionalOnProperty(
            value = "implementation.type",
            havingValue = "advanced")
    public ImplementationTypeManager getAdvancedType(ExternalServicesClient externalServicesClient,
                                                     SparkService sparkService, ExternalInteractor externalInteractor,
                                                     NodesInfoManager nodesInfoManager) {
        return new Advanced(externalServicesClient, sparkService, externalInteractor, nodesInfoManager);
    }

    @Bean
    @ConditionalOnProperty(
            value = "implementation.type",
            havingValue = "local",
            matchIfMissing = true)
    public ImplementationTypeManager getLocalType(BookService bookService, AuthorService authorService, SparkService sparkService) {
        return new Local(bookService, authorService, sparkService);
    }
}
