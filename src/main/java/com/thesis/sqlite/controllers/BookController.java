package com.thesis.sqlite.controllers;

import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.entities.Book;
import com.thesis.sqlite.results.Client;
import com.thesis.sqlite.services.BookService;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("api/books")
@Tag(name = "Books")
public class BookController {
    private final BookService bookService;

    @Autowired
    public BookController(BookService bookService) {
        this.bookService = bookService;
    }

    @GetMapping
    public ResponseEntity<List<Book>> getAll(Pageable pageable) {
        Page<Book> paged = bookService.findAll(pageable);
        return Client.Results.paged(paged, paged.getContent());
    }

    @GetMapping("/count")
    public ResponseEntity<List<BookAuthorCount>> getCount(Pageable pageable) {
        Page<BookAuthorCount> paged = bookService.findBooksGroupedByAuthor(pageable);
        return Client.Results.paged(paged, paged.getContent());
    }

    @PostMapping
    public ResponseEntity<List<Book>> getAllIn(@RequestBody Set<UUID> ids, Pageable pageable) {
        Page<Book> paged = bookService.findAllByAuthorId(ids, pageable);
        return Client.Results.paged(paged, paged.getContent());
    }
}
