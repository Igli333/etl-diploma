package com.diplome.merger.service.implementation;


import com.diplome.merger.service.MergerService;
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

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Log4j2
@RequiredArgsConstructor
public class MergerServiceImplementation implements MergerService {

    private final WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;
    private final DataSource dataSource;

    @Override
    public void merge(TransformationRequest request1, TransformationRequest request2) {
        String workflowId = request1.workflowId();
        String workflowName = request1.workflowName();
        String mainSource = request1.referenceSource();
        String secondarySource = request2.referenceSource();
        String transformationName = request1.transformationName();

        Workflow workflow;
        TransformationResponse response;
        String responseString = "Merger transformation " + transformationName + " for " + workflowName + " %s";


        if (workflowRepository.findById(workflowId).isPresent()) {
            workflow = workflowRepository.findById(workflowId).get();
        } else {
            response = new TransformationResponse(workflowId,
                    workflowName,
                    Transformations.SORTER.name(),
                    String.format(responseString, "failed. Workflow doesn't exist!"),
                    null,
                    null);
            sendResponse(response);
            return;
        }

        Transformation merger = workflow.getTransformations().stream().filter(transformation ->
                        transformation.type().equals(Transformations.MERGER)
                                && transformation.name().equals(transformationName)
                                && transformation.parameters().get("source").equals(mainSource)
                                && transformation.parameters().get("sourceToMerge").equals(secondarySource))
                .toList().get(0);

        try (Connection etl = dataSource.getConnection()) {
            Statement etlStatement = etl.createStatement();

            String mergeQuery = transferQuery(etl, mainSource, secondarySource, merger);

            etlStatement.executeUpdate(mergeQuery);

            response = new TransformationResponse(workflowId,
                    workflowName,
                    transformationName,
                    String.format(responseString, "finished successfully!"),
                    null,
                    List.of(mainSource, secondarySource));

        } catch (SQLException e) {
            log.log(Level.ERROR, e);
            response = new TransformationResponse(workflowId,
                    workflowName,
                    transformationName,
                    null,
                    String.format(responseString, "failed."),
                    null);
        }

        sendResponse(response);
    }

    private String transferQuery(Connection connection, String mainTable, String secondaryTable, Transformation merger) throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO " + mainTable + " (");
        StringBuilder select = new StringBuilder("SELECT ");

        ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, mainTable);
        primaryKeys.next();
        String primaryKey = primaryKeys.getString("COLUMN_NAME");

        Map<String, String> columnsMapping = (HashMap<String, String>) merger.parameters().get("columnMapping");
        Set<String> mainColumns = columnsMapping.keySet();

        for (String mainColumn : mainColumns) {
            String secondaryColumn = columnsMapping.get(mainColumn);
            if (!primaryKey.equals(mainColumn)) {
                insert.append(mainColumn);
                select.append(secondaryColumn);
                insert.append(", ");
                select.append(", ");

            }
        }

        insert.delete(insert.length() - 2, insert.length()).append(") ");
        select.delete(select.length() - 2, select.length()).append(" FROM ").append(secondaryTable).append(";");
        insert.append(select);

        resetSequence(connection, mainTable, primaryKey);

        return insert.toString();
    }

    private void resetSequence(Connection connection, String tableName, String primaryKey) throws SQLException {
        String setInSync = "SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('\"" + tableName + "\"', '"
                + primaryKey + "')), (SELECT (MAX(\"" + primaryKey + "\") + 1) FROM \"" + tableName + "\"), FALSE);";

        Statement syncStatement = connection.createStatement();
        syncStatement.executeQuery(setInSync);
    }

    private void sendResponse(TransformationResponse response) {
        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }
}
