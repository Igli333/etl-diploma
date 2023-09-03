package com.diplome.filter.elements;

public record Filter(String logicalOperator,
                     String filterColumn,
                     String filterRule) {
}
