package com.thesis.sqlite.entities;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;

import java.sql.Types;
import java.util.UUID;

@Entity
@Table(indexes = @Index(name = "fn_index_author", columnList = "author"))
public class Book {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @JdbcTypeCode(Types.VARCHAR)
    private UUID id;
    private String title;
    @JdbcTypeCode(Types.VARCHAR)
    private UUID author;

    public Book(String title, UUID author) {
        this.title = title;
        this.author = author;
    }

    public Book() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public UUID getAuthor() {
        return author;
    }

    public void setAuthor(UUID author) {
        this.author = author;
    }
}
