package com.diplome.extractor.kafka;

import com.diplome.extractor.service.ExtractorService;
import com.diplome.shared.elements.TransformationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ExtractorConsumer {

    private final ExtractorService extractorService;

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).EXTRACTOR.name()}",
            concurrency = "3",
            containerFactory = "kafkaListenerTransformationFactory")
    private void listen(TransformationRequest transformationRequest) {
        extractorService.addDatabaseTableLocally(transformationRequest);
    }
}
