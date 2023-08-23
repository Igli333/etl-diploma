package com.diplome.extractor.kafka;

import com.diplome.extractor.service.implementation.ExtractorServiceImplementation;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private static final String TOPIC = "extraction";

    private final ExtractorServiceImplementation extractorServiceImplementation;

    @KafkaListener(topics = TOPIC, groupId = "group-id")
    private void listen(String workflowId){
        int workflowIdNumber = Integer.parseInt(workflowId);
        extractorServiceImplementation.addDatabaseTableLocally(workflowIdNumber);
    }

}
