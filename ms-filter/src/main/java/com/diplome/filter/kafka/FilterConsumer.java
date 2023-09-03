package com.diplome.filter.kafka;

import com.diplome.filter.service.FilterService;
import com.diplome.shared.elements.TransformationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FilterConsumer {

    private final FilterService filterService;

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).EXCTRACTOR.name()}",
            concurrency = "3",
            containerFactory = "kafkaListenerTransformationFactory")
    private void filterListener(TransformationRequest transformationRequest) {
        filterService.filter(transformationRequest);
    }
}
