package com.thesis.sqlite.dto.request;

import java.util.List;
import java.util.Map;

public record CreateView(String viewName, String tableToUse, Map<String, List<String>> ids) {
}
