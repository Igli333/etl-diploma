package com.diplome.shared.elements;

import java.util.Map;

public record TransformationRequest (String workFlowId, Map<String, Object> parameters){
}
