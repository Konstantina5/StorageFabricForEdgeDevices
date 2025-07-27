package com.thesis.sqlite.confs;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SpringBootSparkApp")
                .setMaster("local[*]");

        return SparkSession.builder()
                .appName("Spring Boot Spark")
                .config(sparkConf)
                .getOrCreate();
    }
}
