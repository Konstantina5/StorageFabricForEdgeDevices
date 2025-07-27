package com.thesis.sqlite.dto;

import java.io.Serializable;
import java.util.UUID;

public class BookAuthorCount implements Serializable {
    private String authorId;
    private Long count;

    public BookAuthorCount(UUID authorId, Long count) {
        this.authorId = String.valueOf(authorId);
        this.count = count;
    }

    public BookAuthorCount() {
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getAuthorId() {
        return authorId;
    }

    public void setAuthorId(String authorId) {
        this.authorId = authorId;
    }
}
