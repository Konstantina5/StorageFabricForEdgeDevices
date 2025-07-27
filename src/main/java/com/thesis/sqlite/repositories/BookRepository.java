package com.thesis.sqlite.repositories;

import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.entities.Book;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.Collection;
import java.util.UUID;

public interface BookRepository extends JpaRepository<Book, UUID> {
    @Query("""
            SELECT new com.thesis.sqlite.dto.BookAuthorCount(b.author, COUNT(b.author))
            FROM com.thesis.sqlite.entities.Book b
            GROUP BY b.author
            ORDER BY b.author""")
    Page<BookAuthorCount> findBooksGroupedByAuthor(Pageable pageable);

    Page<Book> findByAuthorIn(Collection<UUID> ids, Pageable pageable);
}
