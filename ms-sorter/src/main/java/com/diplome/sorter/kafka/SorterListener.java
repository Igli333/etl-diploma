package com.diplome.sorter.kafka;

import com.diplome.shared.elements.TransformationRequest;
import com.diplome.sorter.service.SorterService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SorterListener {
    private final SorterService sorterService;

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).SORTER.name()}",
            concurrency = "3",
            containerFactory = "kafkaListenerTransformationFactory")
    private void sorterListener(TransformationRequest request) {
        sorterService.sort(request);
    }
}
