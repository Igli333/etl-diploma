package com.diplome.shared.elements;

import com.diplome.shared.enums.Transformations;

import java.util.Map;

public record Transformation(Integer queueNumber, Transformations name, String description,
                             Map<String, String> parameters) {
}
