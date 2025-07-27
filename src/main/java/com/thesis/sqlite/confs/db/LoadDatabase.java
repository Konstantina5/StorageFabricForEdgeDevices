package com.thesis.sqlite.confs.db;

import com.thesis.sqlite.entities.Author;
import com.thesis.sqlite.entities.Book;
import com.thesis.sqlite.services.AuthorService;
import com.thesis.sqlite.services.BookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;

@Component
public class LoadDatabase {
    private AuthorService authorService;
    private BookService bookService;

    @Autowired
    public LoadDatabase(AuthorService authorService, BookService bookService) {
        this.authorService = authorService;
        this.bookService = bookService;
        loadDb();
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
