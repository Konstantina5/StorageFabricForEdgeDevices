package com.thesis.sqlite.services;

import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.entities.Author;
import com.thesis.sqlite.repositories.AuthorRepository;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.UUID;

@Service
@Transactional
public class AuthorService {
    private final AuthorRepository authorRepository;

    @Autowired
    public AuthorService(AuthorRepository authorRepository) {
        this.authorRepository = authorRepository;
    }

    public void save(Author author) {
        authorRepository.save(author);
    }

    public Page<BookAuthorCount> findAuthorGrouped(Pageable pageable) {
        return authorRepository.findAuthorGrouped(pageable);
    }

    public Page<Author> findAll(Pageable pageable) {
        return authorRepository.findAll(pageable);
    }

    public Page<Author> findAllById(Set<UUID> commonIds, Pageable pageable) {
        return authorRepository.findByIdIn(commonIds, pageable);
    }
}
