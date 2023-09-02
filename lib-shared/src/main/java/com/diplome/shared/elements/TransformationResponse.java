package com.diplome.shared.elements;

import java.util.List;

public record TransformationResponse(String workflowId, String finishedTransformationName, String message,
                                     String error, List<String> sources) {
}
