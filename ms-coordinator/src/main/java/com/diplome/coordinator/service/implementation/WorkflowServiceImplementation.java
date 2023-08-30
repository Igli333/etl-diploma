package com.diplome.coordinator.service.implementation;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeMap;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class WorkflowServiceImplementation implements WorkflowService {

    private final WorkflowRepository workflowRepository;
    private final ModelMapper modelMapper;
    private final KafkaTemplate<String, TransformationRequest> kafkaTemplate;
    private final Sinks.Many<TransformationResponse> messageSink = Sinks.many().replay().latest();
    private final Map<String, Queue<Transformation>> executingWorkflows = new ConcurrentHashMap<>();

    public Flux<String> startWorkflow(WorkflowDto workflowDto) {
        Workflow workflow = dtoToEntity(workflowDto);
        workflowRepository.save(workflow);

        String workflowId = workflow.getId();
        List<Transformation> transformations = workflow.getTransformations();
        workflow.getSources().forEach(source -> {
            String workflowSourceCompositeKey = workflowId + "-" + source.name();
            Queue<Transformation> transformationsQueue = transformations.stream()
                    .filter(transformation -> !transformation.parameters().get("source").equals(source.name()))
                    .collect(Collectors.toCollection(LinkedList::new));

            executingWorkflows.put(workflowSourceCompositeKey, transformationsQueue);

            Map<String, Object> transformationParameters = new HashMap<>();
            transformationParameters.put("workflowId", workflowId);
            transformationParameters.put("transformationName", Transformations.EXTRACTOR);
            transformationParameters.put("referenceSource", source.name());
            TransformationRequest transformationRequest = new TransformationRequest(workflowId, transformationParameters);

            sendMessage(Transformations.EXTRACTOR, transformationRequest);
        });

        if(transformations.stream().anyMatch(transformation -> transformation.name().equals(Transformations.JOINER))) {
            Queue
        }

        return getUpdatesFromWorkflow(workflowId);
    }

    @Override
    public void endOfTransformation(TransformationResponse transformationResponse) {
        messageSink.tryEmitNext(transformationResponse);

        String workflowId = transformationResponse.workflowId();
        if (transformationResponse.error().isEmpty() || !transformationResponse.message()
                .equals("Loader for workflow: " + workflowId + " finished")) {
            Transformations transformation;
            String workflowSourceCompositeKey = workflowId.substring(workflowId.indexOf("-"));
            Queue<Transformation> transformationQueue = executingWorkflows.get(workflowSourceCompositeKey);

            // You have to check if a JOIN of a MERGE has happened
            // Create a new Queue workflowId-JoinOrMergeResultReference
            // Continue with that as the main workflow
            // For testing purposes, a few branches can merge, but they cannot split
            // Unless it is in the loading stage, which can be done for many target db
            if (transformationQueue == null || transformationQueue.isEmpty()) {
                transformation = Transformations.LOADER;
            } else {
                Transformation nextTransformation = transformationQueue.poll();
                transformation = nextTransformation.name();
            }

            this.sendMessage(transformation, new TransformationRequest(workflowId, new HashMap<>()));
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
        this.modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
        return modelMapper.map(workflowDto, Workflow.class);
    }

    public void sendMessage(Transformations topic, TransformationRequest transformationRequest) {
        kafkaTemplate.send(topic.name(), transformationRequest);
    }

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).list()}")
    private void listen(TransformationResponse transformationResponse) {
        this.endOfTransformation(transformationResponse);
    }

}
