package com.thesis.sqlite.dto;

import java.util.List;
import java.util.Map;

public class QueryResult {
    private Map<String, Integer> columnTypes;
    private List<Map<String, Object>> rows;

    public QueryResult(Map<String, Integer> columnTypes, List<Map<String, Object>> rows) {
        this.columnTypes = columnTypes;
        this.rows = rows;
    }

    public Map<String, Integer> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(Map<String, Integer> columnTypes) {
        this.columnTypes = columnTypes;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }
}
