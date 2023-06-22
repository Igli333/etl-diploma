package com.diplome.shared.enums;

public enum Transformations {
    FILTER("Filter"), JOINER("Joiner"), MERGER("Merger"), SORTER("Sorter");

    private final String name;

    Transformations(final String name) {
        this.name = name;
    }
}
