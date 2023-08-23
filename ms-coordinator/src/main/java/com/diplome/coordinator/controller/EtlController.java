package com.diplome.coordinator.controller;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.stream.Stream;

@RestController
@RequestMapping("etl")
@RequiredArgsConstructor
public class EtlController {

    private final WorkflowService workflowService;

    @PostMapping("/init")
    public Flux<String> initializeWorkflow(@RequestBody WorkflowDto workflow){
        Flux<String> dataStream = Flux.fromStream(() -> Stream.generate(()));
        return dataStream;
    }

}
