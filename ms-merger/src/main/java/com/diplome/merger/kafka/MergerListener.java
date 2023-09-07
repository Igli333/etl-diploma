package com.diplome.merger.kafka;

import com.diplome.merger.service.MergerService;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class MergerListener {

    private final MergerService mergerService;
    private final Map<String, List<TransformationRequest>> messagesForMerge = new ConcurrentHashMap<>();

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).MERGER.name()}",
            concurrency = "3",
            containerFactory = "kafkaListenerTransformationFactory")
    private void sorterListener(TransformationRequest request) {
        int sources = request.size();
        String workflowId = request.workflowId();

        if (!messagesForMerge.containsKey(workflowId)) {
            List<TransformationRequest> requests = new ArrayList<>();
            requests.add(request);
            messagesForMerge.put(request.workflowId(), requests);
            return;
        } else if (sources > messagesForMerge.get(request.workflowId()).size()) {
            messagesForMerge.get(request.workflowId()).add(request);
        }

        if (sources == messagesForMerge.get(request.workflowId()).size()){
            mergerService.merge(messagesForMerge.get(request.workflowId()).toArray(new TransformationRequest[0]));
            messagesForMerge.remove(request.workflowId());
        }
    }
}
