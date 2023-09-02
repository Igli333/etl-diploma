package com.diplome.shared.enums;

import java.util.Arrays;
import java.util.List;

public enum Transformations {
    EXTRACTOR, FILTER, JOINER, MERGER, SORTER, LOADER, RESPONSE;

    public static List<String> list() {
        return Arrays.stream(values()).map(Enum::name).toList();
    }
}
