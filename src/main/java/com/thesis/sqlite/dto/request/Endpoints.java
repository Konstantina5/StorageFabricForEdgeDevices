package com.thesis.sqlite.dto.request;

import org.hibernate.validator.constraints.URL;

import java.util.Set;

public record Endpoints(Set<@URL String> authorEndpoints, Set<@URL String> bookEndpoints) {
}
