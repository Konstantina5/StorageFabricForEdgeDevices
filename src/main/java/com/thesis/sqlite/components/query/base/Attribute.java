package com.thesis.sqlite.components.query.base;

public class Attribute {

    private String name;
    private Type type;
    private Long distinctVals;

    public Attribute(String name, Long distinctVals, Type type) {
        this.name = name;
        this.distinctVals = distinctVals;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Long getDistinctVals() {
        return distinctVals;
    }

    public void setDistinctVals(Long distinctVals) {
        this.distinctVals = distinctVals;
    }

    @Override
    public String toString() {
        return "Attribute{" +
                "name='" + name + '\'' +
                ", distinctVals=" + distinctVals +
                '}';
    }
}
