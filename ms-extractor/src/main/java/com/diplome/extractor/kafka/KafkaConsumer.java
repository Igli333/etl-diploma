package com.diplome.extractor.kafka;

import com.diplome.extractor.service.ExtractorService;
import com.diplome.shared.elements.TransformationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final String TOPIC = "EXTRACTOR";

    private final ExtractorService extractorService;

    @KafkaListener(topics = TOPIC, groupId = "group-id", concurrency = "3")
    private void listen(TransformationRequest transformationRequest){
        extractorService.addDatabaseTableLocally(transformationRequest);
    }

}
