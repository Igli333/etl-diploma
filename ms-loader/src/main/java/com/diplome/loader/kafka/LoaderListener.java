package com.diplome.loader.kafka;

import com.diplome.loader.service.LoaderService;
import com.diplome.shared.elements.TransformationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class LoaderListener {

    private final LoaderService loaderService;
    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).LOADER.name()}",
            containerFactory = "kafkaListenerTransformationFactory")
    private void listen(TransformationRequest transformationRequest) {
        loaderService.loadDatabaseRemotely(transformationRequest);
    }
}
