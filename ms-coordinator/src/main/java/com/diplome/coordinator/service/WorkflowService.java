package com.diplome.coordinator.service;

import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.entities.Workflow;

public interface WorkflowService {
    String startWorkflow(WorkflowDto workflowDto);
}
