package com.thesis.sqlite.results;

import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

public interface Client {
    interface Results {
        static <T> ResponseEntity<T> ok(T t) {
            return ResponseEntity.ok(t);
        }

        static ResponseEntity<?> ok() {
            return new ResponseEntity<>(HttpStatus.OK);
        }

        static <ModelType, TransformedDataType> ResponseEntity<List<TransformedDataType>> paged(Page<ModelType> data,
                                                                                                List<TransformedDataType> transformedDataTypes) {
            return new ResponseEntity<>(transformedDataTypes, com.thesis.sqlite.utils.PaginationUtil.generateHeaders(data), HttpStatus.OK);
        }


        static <ModelType> ResponseEntity<ModelType> paged(ModelType data, long totalElements, int pageSize) {
            return new ResponseEntity<>(data, com.thesis.sqlite.utils.PaginationUtil.generateHeaders(totalElements, pageSize), HttpStatus.OK);
        }
    }

    interface Errors {
        static ResponseEntity<Void> notFound() {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        static ResponseEntity<Void> unprocessableEntity() {
            return new ResponseEntity<>(HttpStatus.UNPROCESSABLE_ENTITY);
        }

        static ResponseEntity<String> internalServerError(String message) {
            return new ResponseEntity<>(message, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
