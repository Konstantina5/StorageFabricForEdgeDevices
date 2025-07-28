package com.thesis.sqlite.confs.db;

import com.thesis.sqlite.entities.Author;
import com.thesis.sqlite.entities.Book;
import com.thesis.sqlite.entities.MetricEntity;
import com.thesis.sqlite.services.AuthorService;
import com.thesis.sqlite.services.BookService;
import com.thesis.sqlite.services.MetricService;
import com.thesis.sqlite.utils.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;

@Component
public class LoadDatabase {
    private AuthorService authorService;
    private BookService bookService;
    private MetricService metricService;
    private JsonUtil jsonUtil;

    @Autowired
    public LoadDatabase(AuthorService authorService, BookService bookService, MetricService metricService, JsonUtil jsonUtil) {
        this.authorService = authorService;
        this.bookService = bookService;
        this.metricService = metricService;
        this.jsonUtil = jsonUtil;
        loadDb();
        loadMetrics();
    }

    private void loadMetrics() {
        String metric = "{\n" +
                "    \"entityType\": \"MEMORY_USAGE\",\n" +
                "    \"unit\": \"PLAIN\",\n" +
                "    \"minVal\": 10,\n" +
                "    \"maxVal\": 10000,\n" +
                "    \"higherIsBetter\": true,\n" +
                "    \"val\": 2000,\n" +
                "    \"timestamp\": 20240305142500,\n" +
                "    \"container\": {\n" +
                "        \"name\": \"name_serv\",\n" +
                "        \"label\": \"server one\"\n" +
                "    }\n" +
                "}";

        MetricEntity metricEntity = jsonUtil.parse(metric, MetricEntity.class);
        metricService.addMetric(metricEntity);
    }

    private void loadDb() {
        IntStream.range(0, 100).forEach(i -> {
            Author author = new Author("test" + i);
            authorService.save(author);
            if(i % 10 == 0) {
                IntStream.range(0, 10).forEach(j -> {
                    Book book = new Book("book" + j, author.getId());
                    bookService.save(book);
                });
            }
        });
    }
}
