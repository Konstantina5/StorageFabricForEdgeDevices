package com.thesis.sqlite.dto;

import java.io.Serializable;

public class JoinResult implements Serializable {
    private String author;
    private String bookAuthor;
    private String bookId;
    private String authorName;
    private String bookTitle;

    public JoinResult(String author, String bookAuthor, String authorName, String bookTitle, String bookId) {
        this.author = author;
        this.bookAuthor = bookAuthor;
        this.bookId = bookId;
        this.authorName = authorName;
        this.bookTitle = bookTitle;
    }

    public JoinResult() {
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getBookId() {
        return bookId;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public String getBookAuthor() {
        return bookAuthor;
    }

    public void setBookAuthor(String bookAuthor) {
        this.bookAuthor = bookAuthor;
    }

    public String getAuthorName() {
        return authorName;
    }

    public void setAuthorName(String authorName) {
        this.authorName = authorName;
    }

    public String getBookTitle() {
        return bookTitle;
    }

    public void setBookTitle(String bookTitle) {
        this.bookTitle = bookTitle;
    }
}
