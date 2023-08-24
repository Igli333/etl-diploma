package com.diplome.coordinator.service.implementation;

import com.diplome.coordinator.kafka.CoordinatorKafkaService;
import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.KafkaTopics;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class WorkflowServiceImplementation implements WorkflowService {

    private final WorkflowRepository workflowRepository;
    private final ModelMapper modelMapper;
    private final CoordinatorKafkaService coordinatorKafkaService;
    private final Sinks.Many<TransformationResponse> messageSink = Sinks.many().replay().latest();
    private final Map<Integer, Queue<Transformation>> executingWorkflows = new ConcurrentHashMap<>();

    public Flux<String> startWorkflow(WorkflowDto workflowDto) {
        Workflow workflow = dtoToEntity(workflowDto);


        Integer savedWorkflow = Mono.fromCallable(() -> workflowRepository.save(workflow))


        executingWorkflows.put(workflow.getId(), new LinkedList<>(workflow.getTransformations()));

        return getUpdatesFromWorkflow(workflow.getId());
    }

    @Override
    public void endOfTransformation(TransformationResponse transformationResponse) {
        messageSink.tryEmitNext(transformationResponse);

        if (transformationResponse.error().isEmpty()) {
            Integer workflowId = transformationResponse.workflowId();
            Transformation nextTransformation = executingWorkflows.get(workflowId).poll();
            if (nextTransformation == null) {
                return;
            }
            coordinatorKafkaService.sendMessage(KafkaTopics.valueOf(nextTransformation.name().name()), String.valueOf(workflowId));
        }
    }

    private Flux<String> getUpdatesFromWorkflow(Integer workflowId) {
        return messageSink.asFlux()
                .filter(message -> !Objects.equals(message.workflowId(), workflowId))
                .takeUntil(message -> message.message().contains("Loader"))
                .map(message -> {
                    if (!message.error().isEmpty()) {
                        return message.error();
                    } else {
                        return message.message();
                    }
                });
    }


    private Workflow dtoToEntity(WorkflowDto workflowDto) {
        return modelMapper.map(workflowDto, Workflow.class);
    }
}
