package com.diplome.joiner.service.implementation;

import com.diplome.joiner.service.JoinerService;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

@Service
@RequiredArgsConstructor
public class JoinerServiceImplementation implements JoinerService {

    private final WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;
    private final DataSource dataSource;

    @Override
    public void join(TransformationRequest... request) {
        TransformationRequest firstRequest = request[0];
        String workflowId = firstRequest.workflowId();
        String workflowName = firstRequest.workflowName();
        String transformationName = firstRequest.transformationName();

        Workflow workflow;
        TransformationResponse response;
        String responseString = "Merger transformation " + transformationName + " for " + workflowName + " %s";

        if (workflowRepository.findById(workflowId).isPresent()) {
            workflow = workflowRepository.findById(workflowId).get();
        } else {
            response = new TransformationResponse(workflowId,
                    workflowName,
                    Transformations.SORTER.name(),
                    String.format(responseString, "failed. Workflow doesn't exist!"),
                    null,
                    null);
            sendResponse(response);
            return;
        }

        Transformation joiner = workflow.getTransformations().stream().filter(transformation ->
                        transformation.type().equals(Transformations.JOINER) &&
                                transformation.name().equals(transformationName))
                .toList().get(0);

    }

    private void sendResponse(TransformationResponse response) {
        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }
}
