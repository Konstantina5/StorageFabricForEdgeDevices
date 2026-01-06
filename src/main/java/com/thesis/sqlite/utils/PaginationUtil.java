package com.thesis.sqlite.utils;

import org.springframework.data.domain.Page;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

public class PaginationUtil {
    public static <ModelType> MultiValueMap<String, String> generateHeaders(Page<ModelType> data) {
        LinkedMultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
        headers.add(HeaderNames.PAGE_INDEX, Integer.toString(data.getNumber()));
        headers.add(HeaderNames.PAGE_SIZE, Integer.toString(data.getSize()));
        headers.add(HeaderNames.PAGE_COUNT, Integer.toString(data.getTotalPages()));
        headers.add(HeaderNames.ITEM_COUNT, Long.toString(data.getTotalElements()));
        headers.add(HeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, String.join(",",
                HeaderNames.PAGE_INDEX, HeaderNames.PAGE_COUNT, HeaderNames.PAGE_SIZE, HeaderNames.ITEM_COUNT));
        return headers;
    }

    public static MultiValueMap<String, String> generateHeaders(long totalElements, int pageSize) {
        LinkedMultiValueMap<String, String> headers = new LinkedMultiValueMap<>();

        int totalPages = (int) Math.ceil((double) totalElements / pageSize);
        headers.add(HeaderNames.PAGE_COUNT, String.valueOf(totalPages));
        headers.add(HeaderNames.PAGE_SIZE, Integer.toString(pageSize));

        return headers;
    }

    public interface HeaderNames {
        String ACCESS_CONTROL_EXPOSE_HEADERS = "Access-Control-Expose-Headers";
        String PAGE_INDEX = "X-Pagination-Index";
        String PAGE_COUNT = "X-Pagination-Count";
        String ITEM_COUNT = "X-Pagination-Items-Count";
        String PAGE_SIZE = "X-Pagination-Size";
    }
}
