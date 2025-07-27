package com.thesis.sqlite.services;

import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.entities.Book;
import com.thesis.sqlite.repositories.BookRepository;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.UUID;

@Service
@Transactional
public class BookService {
    private final BookRepository bookRepository;

    @Autowired
    public BookService(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    public void save(Book book) {
        bookRepository.save(book);
    }

    public Page<Book> findAll(Pageable pageable) {
        return bookRepository.findAll(pageable);
    }

    public Page<BookAuthorCount> findBooksGroupedByAuthor(Pageable pageable) {
        return bookRepository.findBooksGroupedByAuthor(pageable);
    }

    public Page<Book> findAllByAuthorId(Set<UUID> commonIds, Pageable pageable) {
        return bookRepository.findByAuthorIn(commonIds, pageable);
    }

}
