package com.thesis.sqlite.dto.streaming;

import java.util.Optional;

public class Field {
    private String fieldName;
    private Type type;
    private Generation generation;
    private Integer min;
    private Integer max;
    private Object currentValue;

    public Field(String fieldName, Type type, Generation generation, Integer min, Integer max) {
        this.fieldName = fieldName;
        this.type = type;
        this.generation = generation;
        this.min = min;
        this.max = max;
        Optional.ofNullable(min)
                .ifPresent(minVal -> currentValue = minVal);
    }

    public Generation getGeneration() {
        return generation;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setCurrentValue(Object currentValue) {
        this.currentValue = currentValue;
    }

    public Type getType() {
        return type;
    }

    public Integer getMin() {
        return min;
    }

    public Integer getMax() {
        return max;
    }

    public Object getCurrentValue() {
        return currentValue;
    }

    public enum Type {
        INTEGER,
        FLOAT,
        STRING,
    }

    public enum Generation {
        INCREASING,
        RANDOM
    }
}
