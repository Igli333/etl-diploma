package com.diplome.shared.elements;

import java.util.Map;

public record TransformationRequest(String workflowId,
                                    String workflowName,
                                    String referenceSource,
                                    String transformationName,
                                    int size) {
}
