package com.diplome.coordinator.service.implementation;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.dto.WorkflowDto;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Log4j2
@RequiredArgsConstructor
public class WorkflowServiceImplementation implements WorkflowService {

    private final WorkflowRepository workflowRepository;
    private final ModelMapper modelMapper;
    private final KafkaTemplate<String, TransformationRequest> kafkaTemplate;
    private final Sinks.Many<TransformationResponse> messageSink = Sinks.many().replay().latest();
    private final Map<String, Queue<Transformation>> executingWorkflows = new ConcurrentHashMap<>();

    @Override
    public Flux<String> startWorkflow(WorkflowDto workflowDto) {
        Workflow workflow = dtoToEntity(workflowDto);
        try {
            workflowRepository.save(workflow);
        } catch (Exception e) {
            log.log(Level.ERROR, e);
            return Flux.error(e);
        }

        String workflowId = workflow.getId();
        List<Transformation> transformations = workflow.getTransformations();
        workflow.getSources().forEach(source -> {
            String workflowSourceCompositeKey = workflowId + "-" + source.name();
            Queue<Transformation> transformationsQueue = transformations.stream()
                    .filter(transformation -> !transformation.parameters().get("source").equals(source.name()))
                    .collect(Collectors.toCollection(LinkedList::new));

            executingWorkflows.put(workflowSourceCompositeKey, transformationsQueue);
            sendMessage(Transformations.EXTRACTOR, new TransformationRequest(workflowId, source.name(), ""));
        });

        List<Transformation> compositeTransformations = transformations.stream().filter(transformation ->
                transformation.type().equals(Transformations.JOINER) || transformation.type().equals(Transformations.MERGER)).toList();

        if (!compositeTransformations.isEmpty()) {
            compositeTransformations.forEach(compositeTransformation -> {
                Queue<Transformation> afterJoin = transformations.stream()
                        .filter(transformation -> transformation.parameters().get("source").equals(compositeTransformation.name()))
                        .collect(Collectors.toCollection(LinkedList::new));

                executingWorkflows.put(workflowId + "-" + compositeTransformation.name(), afterJoin);
            });
        }

        return getUpdatesFromWorkflow(workflowId);
    }

    @Override
    public void endOfTransformation(TransformationResponse transformationResponse) {
        messageSink.tryEmitNext(transformationResponse);

        if (transformationResponse.error() != null) {
            return;
        }

        String workflowId = transformationResponse.workflowId();
        if (!transformationResponse.message().equals("Loader for workflow: " + workflowId + " finished")) {
            String referenceSource = transformationResponse.sources().get(0);

            if (transformationResponse.sources().size() > 1) {
                referenceSource = transformationResponse.finishedTransformationName();
            }

            String workflowSourceCompositeKey = transformationResponse.workflowId() + "-" + referenceSource;
            Queue<Transformation> transformationQueue = executingWorkflows.get(workflowSourceCompositeKey);

            if (!transformationQueue.isEmpty()) {
                Transformation nextTransformation = transformationQueue.poll();
                sendMessage(nextTransformation.type(), new TransformationRequest(workflowId, referenceSource, nextTransformation.name()));
            } else {
                sendMessage(Transformations.LOADER, new TransformationRequest(workflowId, referenceSource, ""));
            }
        } else {
            messageSink.tryEmitNext(new TransformationResponse(workflowId,
                    workflowRepository.findById(workflowId).get().getWorkflowName(),
                    "Workflow with id: " + workflowId + " has finished successfully!",
                    null, null));
        }

    }

    private Flux<String> getUpdatesFromWorkflow(String workflowId) {
        return messageSink.asFlux()
                .filter(message -> Objects.equals(message.workflowId(), workflowId))
                .takeUntil(message -> message.error() == null || message.message().equals("Workflow with id: " + workflowId + " has finished successfully!"))
                .map(message -> {
                    if (message.error() != null) {
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

    private void sendMessage(Transformations topic, TransformationRequest transformationRequest) {
        kafkaTemplate.send(topic.name(), transformationRequest);
    }


}
