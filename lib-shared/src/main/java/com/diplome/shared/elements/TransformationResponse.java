package com.diplome.shared.elements;

public record TransformationResponse(String workflowId, String finishedTransformationName, String message,
                                     String error) {
}
