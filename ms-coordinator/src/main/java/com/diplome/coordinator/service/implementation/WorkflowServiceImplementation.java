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

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Log4j2
@RequiredArgsConstructor
public class WorkflowServiceImplementation implements WorkflowService {

    private final WorkflowRepository workflowRepository;
    private final ModelMapper modelMapper;
    private final DataSource dataSource;
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
            String sourceName = source.name();
            String workflowSourceCompositeKey = workflowId + "-" + sourceName;
            Queue<Transformation> transformationsQueue = generateExecutionFlow(transformations, sourceName);

            executingWorkflows.put(workflowSourceCompositeKey, transformationsQueue);
            sendMessage(Transformations.EXTRACTOR, new TransformationRequest(workflowId, workflow.getWorkflowName(), sourceName, "", 1));
        });

        List<Transformation> compositeTransformations = transformations.stream().filter(transformation ->
                transformation.type().equals(Transformations.JOINER) || transformation.type().equals(Transformations.MERGER)).toList();

        if (!compositeTransformations.isEmpty()) {
            compositeTransformations.forEach(compositeTransformation -> {
                String src = compositeTransformation.name();
                Queue<Transformation> afterJoinOrMerge = generateExecutionFlow(transformations, src);

                executingWorkflows.put(workflowId + "-" + compositeTransformation.name(), afterJoinOrMerge);
            });
        }

        return getUpdatesFromWorkflow(workflowId);
    }

    @Override
    public void endOfTransformation(TransformationResponse transformationResponse) {
        String workflowId = transformationResponse.workflowId();
        String workflowName = transformationResponse.workflowName();

        if (transformationResponse.error() != null) {
            messageSink.tryEmitNext(transformationResponse);
            messageSink.tryEmitNext(new TransformationResponse(workflowId,
                    transformationResponse.workflowName(),
                    "WORKFLOW",
                    null,
                    "Workflow with name: " + workflowName + " has failed!",
                    null));
            return;
        }

        messageSink.tryEmitNext(transformationResponse);

        try {
            if (!transformationResponse.message().contains("Loader")) {
                String referenceSource = transformationResponse.sources().get(0);

                if (transformationResponse.sources().size() > 1) {
                    referenceSource = transformationResponse.finishedTransformationName();
                }

                String workflowSourceCompositeKey = transformationResponse.workflowId() + "-" + referenceSource;
                Queue<Transformation> transformationQueue = executingWorkflows.get(workflowSourceCompositeKey);

                if (!transformationQueue.isEmpty()) {
                    Transformation nextTransformation = transformationQueue.poll();
                    int size = 1;
                    if (nextTransformation.parameters().containsKey("secondarySources")) {
                        size = ((List<String>) nextTransformation.parameters().get("secondarySources")).size() + 1;
                    }

                    if (nextTransformation.parameters().containsKey("joinSources")) {
                        size = ((List<Object>) nextTransformation.parameters().get("joinSources")).size();
                    }
                    sendMessage(nextTransformation.type(), new TransformationRequest(workflowId, workflowName, referenceSource, nextTransformation.name(), size));
                } else {
                    sendMessage(Transformations.LOADER, new TransformationRequest(workflowId, workflowName, referenceSource, "", 1));
                }
            } else {
                messageSink.tryEmitNext(new TransformationResponse(workflowId,
                        transformationResponse.workflowName(),
                        "WORKFLOW",
                        "Workflow with name: " + workflowName + " has finished successfully!",
                        null,
                        null));
            }
        } catch (Exception e) {
            log.log(Level.ERROR, e);
            messageSink.tryEmitNext(new TransformationResponse(workflowId,
                    transformationResponse.workflowName(),
                    "WORKFLOW",
                    null,
                    "Workflow with name: " + workflowName + " has failed!",
                    null));
        }
    }

    private Flux<String> getUpdatesFromWorkflow(String workflowId) {
        return messageSink.asFlux()
                .filter(message -> Objects.equals(message.workflowId(), workflowId))
                .takeUntil(message -> message.finishedTransformationName().equals("WORKFLOW"))
                .map(message -> {
                    if (message.error() != null) {
                        return message.error();
                    } else {
                        return message.message();
                    }
                }).doFinally((signalType) -> {
                    log.log(Level.INFO, "Workflow Status: " + signalType);
                    clearWorkflowFromRuntime(workflowId);
                });
    }

    private Workflow dtoToEntity(WorkflowDto workflowDto) {
        this.modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
        return modelMapper.map(workflowDto, Workflow.class);
    }

    private Queue<Transformation> generateExecutionFlow(List<Transformation> transformations, String source) {
        return transformations.stream()
                .filter(transformation -> {
                    Map<String, Object> parameters = transformation.parameters();
                    boolean hasMergeForward = parameters.containsKey("secondarySources");
                    boolean hasJoinForward = parameters.containsKey("joinSources");
                    return parameters.containsKey("source") && parameters.get("source").equals(source) ||
                            (hasMergeForward && ((List<Map<String, Object>>) parameters.get("secondarySources"))
                                    .stream().anyMatch(ref -> ref.get("name").equals(source))) ||
                            (hasJoinForward && ((List<Map<String, Object>>) parameters.get("joinSources"))
                                    .stream().anyMatch(ref -> ref.get("name").equals(source)));
                })
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private void sendMessage(Transformations topic, TransformationRequest transformationRequest) {
        kafkaTemplate.send(topic.name(), transformationRequest);
    }

    private void clearWorkflowFromRuntime(String workflowId) {
        Set<String> keys = executingWorkflows.keySet();
        try (Connection etl = dataSource.getConnection()) {
            Statement etlStatement = etl.createStatement();
            DatabaseMetaData metadata = etl.getMetaData();

            for (String key : keys) {
                if (key.startsWith(workflowId)) {
                    String tableName = key.substring(key.indexOf('-') + 1, key.length());
                    executingWorkflows.remove(key);

                    ResultSet tables = metadata.getTables(null,
                            null,
                            tableName + "%",
                            new String[]{"TABLE"});

                    while (!tables.isClosed() && tables.next()) {
                        etlStatement.executeUpdate("DROP TABLE " + tables.getString("table_name") + ";");
                    }

                    etlStatement.executeUpdate("DROP SEQUENCE IF EXISTS " + tableName + "_increment_sequence;");
                }
            }
        } catch (SQLException e) {
            log.log(Level.ERROR, e);
        }
    }
}
