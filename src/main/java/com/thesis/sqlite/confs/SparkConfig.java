package com.thesis.sqlite.confs;

import com.thesis.sqlite.utils.Utils;
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

//        return SparkSession.builder()
//                .appName("Spring Boot Spark")
//                .config(sparkConf)
//                .getOrCreate();

        return SparkSession.builder()
                .appName("SpringBootSparkApp")
                .master("spark://spark-master:7077")
                .config("spark.driver.host", Utils.HOSTNAME)  // automatically uses container hostname
                .getOrCreate();

//        return SparkSession.builder()
//                .remote("sc://spark-master:15002")
//                .getOrCreate();
    }
}
