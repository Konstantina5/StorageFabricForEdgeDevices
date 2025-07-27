package com.thesis.sqlite.components.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.BiFunction;

@Service
public class SparkService {
    private final SparkSession sparkSession;

    @Autowired
    public SparkService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> performJoin(Dataset<Row> dataset1, Dataset<Row> dataset2,
                                    BiFunction<Dataset<Row>, Dataset<Row>, Column> joinFunc) {
//        Dataset<Row> joined = dataset1.join(dataset2, dataset2.col("author_id").equalTo(dataset1.col("book_author")));
        Dataset<Row> joined = dataset1.join(dataset2, joinFunc.apply(dataset2, dataset1));

        long count = joined.count();
        System.out.println(count);

        return joined;
    }

    public SparkSession getSpark() {
        return sparkSession;
    }
}
