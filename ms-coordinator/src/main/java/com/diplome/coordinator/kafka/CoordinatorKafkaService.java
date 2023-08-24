package com.diplome.coordinator.kafka;

import com.diplome.coordinator.service.WorkflowService;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.enums.KafkaTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

@Service
@Component
@RequiredArgsConstructor
public class CoordinatorKafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final WorkflowService workflowService;

    public void sendMessage(KafkaTopics topic, String workflowId) {
        kafkaTemplate.send(topic.name(), workflowId);
    }

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.KafkaTopics).list()}")
    public void listen(TransformationResponse transformationResponse) {
        workflowService.endOfTransformation(transformationResponse);
    }
}
