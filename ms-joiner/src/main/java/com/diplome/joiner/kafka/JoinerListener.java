package com.diplome.joiner.kafka;

import com.diplome.joiner.service.JoinerService;
import com.diplome.shared.elements.TransformationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class JoinerListener {
    private final JoinerService joinerService;
    private final Map<String, List<TransformationRequest>> messagesForJoin = new ConcurrentHashMap<>();

    @KafkaListener(topics = "#{T(com.diplome.shared.enums.Transformations).JOINER.name()}",
            concurrency = "3",
            containerFactory = "kafkaListenerTransformationFactory")
    private void joinerListener(TransformationRequest request) {
        int sources = request.size();
        String workflowId = request.workflowId();

        if (!messagesForJoin.containsKey(workflowId)) {
            List<TransformationRequest> requests = new ArrayList<>();
            requests.add(request);
            messagesForJoin.put(request.workflowId(), requests);
            return;
        } else if (sources > messagesForJoin.get(request.workflowId()).size()) {
            messagesForJoin.get(request.workflowId()).add(request);
        }

        if (sources == messagesForJoin.get(request.workflowId()).size()){
            joinerService.join(messagesForJoin.get(request.workflowId()).toArray(new TransformationRequest[0]));
            messagesForJoin.remove(request.workflowId());
        }
    }
}
