package com.thesis.sqlite.components.query.base;

public enum Type {
    UUID("uuid"),
    CHAR("varchar(255)"),
    INT("int"),;

    public final String label;

    Type(String label) {
        this.label = label;
    }
}
