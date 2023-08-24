package com.diplome.shared.enums;

import java.util.Arrays;
import java.util.List;

public enum KafkaTopics {
    EXTRACTOR("extractor"), FILTER("filter"), JOINER("joiner"), MERGER("merger"), SORTER("sorter"), LOADER("loader");

    KafkaTopics(final String name) {
    }

    public static List<String> list() {
        return Arrays.stream(values()).map(Enum::name).toList();
    }
}
