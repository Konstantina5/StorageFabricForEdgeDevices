package com.thesis.sqlite.components.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DatasetsUtils {

    public static <T> Dataset<T> createDataset(SparkSession sparkSession,
                                               Function<Pageable, Page<T>> getPageableResults,
                                               Pageable pageable, Encoder<T> encoder) {
        Page<T> paged = getPageableResults.apply(pageable);
        Dataset<T> newDataset = createDataset(sparkSession, paged.getContent(), encoder);

        while (paged.hasNext()) {
            pageable = paged.nextPageable();
            paged = getPageableResults.apply(pageable);
            newDataset = newDataset.union(createDataset(sparkSession, paged.getContent(), encoder));
        }

        return newDataset;
    }

    public static <T> Dataset<T> createDataset(SparkSession sparkSession, List<T> inputList, Encoder<T> encoder) {
        return sparkSession.createDataset(inputList, encoder);
    }

    public static Dataset<Row> createDataset(SparkSession sparkSession, List<JsonNode> list) {
        ObjectMapper objectMapper = new ObjectMapper();//TODO inject
        List<String> stringList = list.stream()
                .map(json -> {
                    try {
                        return objectMapper.writeValueAsString(json);
                    } catch (JsonProcessingException ex) {
                        throw new RuntimeException(ex);
                    }
                }).toList();
        Dataset<String> dataset = sparkSession.createDataset(stringList, Encoders.STRING());
        Dataset<Row> df = sparkSession.read().json(dataset);

        return df;
    }

    public static Dataset<Row> unionAllDatasets(List<Dataset<Row>> datasets) {
        return datasets.stream()
                .reduce(Dataset::union)
                .orElseThrow();
    }

    public static Dataset<Row> renameDatasetColumns(Dataset<Row> initialDataset, Map<String, String> renamedColumns) {
        return renamedColumns.entrySet().stream()
                .reduce(initialDataset,
                        (dataset, entry) -> dataset.withColumnRenamed(entry.getKey(), entry.getValue()),
                        (d1, d2) -> d1);
    }

    public static Map<String, String> authorRenamedColumns() {
        return Map.of("authorId", "author_id", "count", "set1");
    }

    public static Map<String, String> bookRenamedColumns() {
        return Map.of("authorId", "book_author", "count", "set2");
    }
}
