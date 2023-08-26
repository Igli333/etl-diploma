package com.diplome.coordinator.service.implementation;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

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

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Sinks.Many<TransformationResponse> messageSink = Sinks.many().replay().latest();
    private final Map<String, Queue<Transformation>> executingWorkflows = new ConcurrentHashMap<>();

    public Flux<String> startWorkflow(WorkflowDto workflowDto) {
        Workflow workflow = workflowRepository.save(dtoToEntity(workflowDto));
        String workflowId = workflow.getId();

        executingWorkflows.put(workflowId, new LinkedList<>(workflow.getTransformations()));
        return getUpdatesFromWorkflow(workflowId);
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
            this.sendMessage(Transformations.valueOf(nextTransformation.name().name()), String.valueOf(workflowId));
        }
    }

    private Flux<String> getUpdatesFromWorkflow(String workflowId) {
        return messageSink.asFlux()
                .filter(message -> !Objects.equals(message.workflowId(), workflowId))
                .takeUntil(message -> message.message().equals("Loader for workflow: " + workflowId + " finished"))
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

    public void sendMessage(Transformations topic, String workflowId) {
        kafkaTemplate.send(topic.name(), workflowId);
    }

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).list()}")
    private void listen(TransformationResponse transformationResponse) {
        this.endOfTransformation(transformationResponse);
    }

}
