package com.thesis.sqlite.controllers;

import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.entities.Author;
import com.thesis.sqlite.results.Client;
import com.thesis.sqlite.services.AuthorService;
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
@RequestMapping("api/authors")
@Tag(name = "Authors")
public class AuthorController {
    private final AuthorService authorService;

    @Autowired
    public AuthorController(AuthorService authorService) {
        this.authorService = authorService;
    }

    @GetMapping
    public ResponseEntity<List<Author>> getAll(Pageable pageable) {
        Page<Author> paged = authorService.findAll(pageable);
        return Client.Results.paged(paged, paged.getContent());
    }

    @GetMapping("/count")
    public ResponseEntity<List<BookAuthorCount>> getCount(Pageable pageable) {
        Page<BookAuthorCount> paged = authorService.findAuthorGrouped(pageable);
        return Client.Results.paged(paged, paged.getContent());
    }

    @PostMapping
    public ResponseEntity<List<Author>> getAllIn(@RequestBody Set<UUID> ids, Pageable pageable) {
        Page<Author> paged = authorService.findAllById(ids, pageable);
        return Client.Results.paged(paged, paged.getContent());
    }
}
