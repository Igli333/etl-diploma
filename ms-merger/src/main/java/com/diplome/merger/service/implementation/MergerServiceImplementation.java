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
import java.util.*;
import java.util.stream.Collectors;

@Service
@Log4j2
@RequiredArgsConstructor
public class MergerServiceImplementation implements MergerService {

    private final WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;
    private final DataSource dataSource;

    @Override
    public void merge(TransformationRequest... request) {
        TransformationRequest firstRequest = request[0];
        String workflowId = firstRequest.workflowId();
        String workflowName = firstRequest.workflowName();
        String transformationName = firstRequest.transformationName();

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
                        transformation.type().equals(Transformations.MERGER) &&
                                transformation.name().equals(transformationName))
                .toList().get(0);

        String mainSource = merger.parameters().get("source").toString();
        List<Map<String, Object>> secondarySources = ((List<Map<String, Object>>) merger.parameters().get("secondarySources"));

        try (Connection etl = dataSource.getConnection()) {
            Statement etlStatement = etl.createStatement();

            for (Map<String, Object> sndSource : secondarySources) {
                String sourceName = (String) sndSource.get("name");
                String mergeQuery = transferQuery(etl,
                        mainSource,
                        sourceName,
                        (Map<String, String>) sndSource.get("columnMapping"));

                etlStatement.executeUpdate(mergeQuery);

                etlStatement.executeUpdate("DROP TABLE " + sourceName);
            }

            String renameTable = "ALTER TABLE " + mainSource + " RENAME TO " + transformationName + " ;";

            etlStatement.executeUpdate(renameTable);

            List<String> refSources = ((List<Object>) merger.parameters().get("secondarySources"))
                    .stream().map(sources -> (String) ((Map<String, Object>) sources).get("name")).collect(Collectors.toList());
            refSources.add(0, mainSource);

            response = new TransformationResponse(workflowId,
                    workflowName,
                    transformationName,
                    String.format(responseString, "finished successfully!"),
                    null,
                    refSources);

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

    private String transferQuery(Connection connection, String mainTable, String secondaryTable, Map<String, String> columnsMapping) throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO " + mainTable + " (");
        StringBuilder select = new StringBuilder("SELECT ");

        ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, mainTable);
        primaryKeys.next();
        String primaryKey = primaryKeys.getString("COLUMN_NAME");

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
