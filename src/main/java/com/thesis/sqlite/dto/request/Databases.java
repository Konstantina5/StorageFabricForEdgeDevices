package com.thesis.sqlite.dto.request;

import java.util.Set;

public record Databases(Set<String> authorDatabases, Set<String> bookDatabases) {
}
