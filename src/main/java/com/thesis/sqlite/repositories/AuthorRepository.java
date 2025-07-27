package com.thesis.sqlite.repositories;

import com.thesis.sqlite.dto.BookAuthorCount;
import com.thesis.sqlite.entities.Author;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.UUID;

@Repository
public interface AuthorRepository extends JpaRepository<Author, UUID> {
    @Query("""
            SELECT new com.thesis.sqlite.dto.BookAuthorCount(a.id, COUNT(a.id))
            FROM com.thesis.sqlite.entities.Author a
            GROUP BY a.id
            ORDER BY a.id""")
    Page<BookAuthorCount> findAuthorGrouped(Pageable pageable);

    Page<Author> findByIdIn(Collection<UUID> ids, Pageable pageable);
}
