package com.thesis.sqlite.dto;

import java.util.List;

public record ResultWithTime(long executionTime, List<String> result) {
}
