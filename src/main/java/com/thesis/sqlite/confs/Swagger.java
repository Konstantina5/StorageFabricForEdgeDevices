package com.thesis.sqlite.confs;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Swagger {

    @Bean
    public OpenAPI defineOpenApi() {
        Info information = new Info()
                .version("1.0");
        return new OpenAPI().info(information);
    }

}
