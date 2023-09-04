package com.diplome.filter.service.implementation;

import com.diplome.filter.elements.Filter;
import com.diplome.filter.service.FilterService;
import com.diplome.shared.configuration.Mapper;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

@Service
@Log4j2
@RequiredArgsConstructor
public class FilterServiceImplementation implements FilterService {
    private final DataSource dataSource;
    private final WorkflowRepository workflowRepository;
    private final Mapper mapper;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;

    @Override
    @Transactional
    public void filter(TransformationRequest request) {
        String workflowId = request.workflowId();
        String referenceSource = request.referenceSource();
        String transformationName = request.transformationName();

        Workflow workflow;
        TransformationResponse response;
        String responseString = "Filter transformation %s for source: %s %s";
        if (workflowRepository.findById(workflowId).isPresent()) {
            workflow = workflowRepository.findById(workflowId).get();
        } else {
            response = new TransformationResponse(workflowId,
                    "",
                    null,
                    String.format(responseString, transformationName, referenceSource, "failed. Workflow doesn't exist!"),
                    null);
            sendResponse(response);
            return;
        }

        Transformation filter = workflow.getTransformations().stream().filter(transformation ->
                        transformation.type().equals(Transformations.FILTER)
                                && transformation.name().equals(transformationName)
                                && transformation.parameters().get("source").equals(referenceSource))
                .toList().get(0);

        String filterQuery = createFilterQuery(referenceSource, filter);

        try (Connection etlDb = dataSource.getConnection()) {
            Statement filterStatement = etlDb.createStatement();
            filterStatement.executeUpdate(filterQuery);

            response = new TransformationResponse(workflowId, transformationName,
                    String.format(responseString, transformationName, referenceSource, "finished!"),
                    null,
                    List.of(referenceSource));
        } catch (SQLException e) {
            log.log(Level.ERROR, e);
            response = new TransformationResponse(workflowId,
                    transformationName,
                    null,
                    String.format(responseString, transformationName, referenceSource, "failed."),
                    null);
        }

        sendResponse(response);
    }


    private String createFilterQuery(String tableName, Transformation filter) {
        List<Filter> filters = mapper.map((List<?>) filter.parameters().get("filters"), Filter.class);

        StringBuilder filterQueryBuilder = new StringBuilder("DELETE FROM " + tableName + " WHERE ");

        filters.forEach(flt -> {
            String operator = flt.getLogicalOperator();
            if (operator != null && !Objects.equals(operator, "")) {
                filterQueryBuilder.append(operator).append(" ");
            }

            filterQueryBuilder.append(flt.getFilterColumn()).append(" ").append(flt.getFilterRule()).append(" ");
        });

        filterQueryBuilder.append(";");

        return filterQueryBuilder.toString();
    }

    private void sendResponse(TransformationResponse response) {
        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }
}
