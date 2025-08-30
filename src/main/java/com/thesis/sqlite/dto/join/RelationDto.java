package com.thesis.sqlite.dto.join;

public class RelationDto {
    private String baseUrl;
    private String name;
    private String shortName;
    private long rowCount;

    public RelationDto(String baseUrl, String name, String shortName, long rowCount) {
        this.baseUrl = baseUrl;
        this.name = name;
        this.shortName = shortName;
        this.rowCount = rowCount;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }
}
