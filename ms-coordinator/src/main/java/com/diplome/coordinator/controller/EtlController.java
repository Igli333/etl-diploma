package com.diplome.coordinator.controller;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/etl")
@RequiredArgsConstructor
public class EtlController {

    private final WorkflowService workflowService;

    @PostMapping(path = "/init", produces = "text/event-stream")
    public Flux<String> initializeWorkflow(@RequestBody WorkflowDto workflow) {
        return workflowService.startWorkflow(workflow);
    }

}
