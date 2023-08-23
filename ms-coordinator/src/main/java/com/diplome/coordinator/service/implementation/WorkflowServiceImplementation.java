package com.diplome.coordinator.service.implementation;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class WorkflowServiceImplementation implements WorkflowService {

    private final WorkflowRepository workflowRepository;
    private final ModelMapper modelMapper;

    public String startWorkflow(WorkflowDto workflowDto){
        Workflow workflow = dtoToEntity(workflowDto);
        workflowRepository.save(workflow);

        return "Workflow with name " + workflow.getWorkflowName() + " started!";
    }


    private Workflow dtoToEntity(WorkflowDto workflowDto) {
        return modelMapper.map(workflowDto, Workflow.class);
    }
}
