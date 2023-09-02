package com.diplome.coordinator.kafka;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.elements.TransformationResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CoordinatorConsumer {

    private final WorkflowService workflowService;

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).RESPONSE.name()}", containerFactory = "kafkaListenerCoordinatorFactory")
    private void listen(TransformationResponse transformationResponse) {
        workflowService.endOfTransformation(transformationResponse);
    }
}
