package com.thesis.sqlite.dto.streaming;

public record StreamingResult(long executionTime, long transferDataTime, long resultSize, long totalTime) {
}
