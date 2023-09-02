package com.diplome.coordinator.service;

import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import reactor.core.publisher.Flux;

public interface WorkflowService {
    Flux<String> startWorkflow(WorkflowDto workflowDto);

    void endOfTransformation(TransformationResponse transformationResponse);
}
