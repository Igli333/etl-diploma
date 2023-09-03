package com.diplome.shared.elements;

import com.diplome.shared.enums.Transformations;

import java.util.Map;

public record Transformation(Transformations type,
                             String name,
                             String description,
                             Map<String, Object> parameters) {
}
