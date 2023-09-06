package com.diplome.merger.kafka;

import com.diplome.merger.service.MergerService;
import com.diplome.shared.elements.TransformationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class MergerListener {

    private final MergerService mergerService;
    private final Map<String, TransformationRequest> messagesForMerge = new ConcurrentHashMap<>();

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).MERGER.name()}",
            concurrency = "3",
            containerFactory = "kafkaListenerTransformationFactory")
    private void sorterListener(TransformationRequest request) {
        if (!messagesForMerge.containsKey(request.workflowId())) {
            messagesForMerge.put(request.workflowId(), request);
        } else {
            mergerService.merge(messagesForMerge.get(request.workflowId()), request);
            messagesForMerge.remove(request.workflowId());
        }
    }
}
