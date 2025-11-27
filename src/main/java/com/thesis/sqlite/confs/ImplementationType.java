package com.thesis.sqlite.confs;

import com.thesis.sqlite.algorithm.modules.Advanced;
import com.thesis.sqlite.algorithm.modules.base.ImplementationTypeManager;
import com.thesis.sqlite.components.ExternalServicesClient;
import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.components.spark.SparkService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ImplementationType {

    @Bean
    public ImplementationTypeManager getAdvancedType(ExternalServicesClient externalServicesClient,
                                                     SparkService sparkService, ExternalInteractor externalInteractor,
                                                     NodesInfoManager nodesInfoManager) {
        return new Advanced(externalServicesClient, sparkService, externalInteractor, nodesInfoManager);
    }
}
