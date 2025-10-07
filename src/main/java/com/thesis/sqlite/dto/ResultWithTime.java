package com.thesis.sqlite.dto;

import java.util.List;

public class ResultWithTime {
    private final long executionTime;
    private final List<String> result;

    public ResultWithTime(long executionTime, List<String> result) {
        this.executionTime = executionTime;
        this.result = result;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public List<String> getResult() {
        return result;
    }
}
