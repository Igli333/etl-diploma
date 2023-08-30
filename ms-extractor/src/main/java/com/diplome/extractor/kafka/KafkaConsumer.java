package com.diplome.extractor.kafka;

import com.diplome.extractor.service.implementation.ExtractorServiceImplementation;
import com.diplome.shared.enums.Transformations;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final String TOPIC = "EXTRACTOR";

    private final ExtractorServiceImplementation extractorServiceImplementation;

    @KafkaListener(topics = TOPIC, groupId = "group-id", concurrency = "3")
    private void listen(String workflowId){
        extractorServiceImplementation.addDatabaseTableLocally(workflowId);
    }

}
