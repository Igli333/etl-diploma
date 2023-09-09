package com.diplome.joiner.service.implementation;

import com.diplome.joiner.elements.JoinSource;
import com.diplome.joiner.service.JoinerService;
import com.diplome.shared.configuration.Mapper;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Service
@RequiredArgsConstructor
public class JoinerServiceImplementation implements JoinerService {

    private final WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;
    private final DataSource dataSource;
    private final Mapper mapper;

    @Override
    public void join(TransformationRequest... request) {
        TransformationRequest firstRequest = request[0];
        String workflowId = firstRequest.workflowId();
        String workflowName = firstRequest.workflowName();
        String transformationName = firstRequest.transformationName();

        Workflow workflow;
        TransformationResponse response;
        String responseString = "Joiner transformation " + transformationName + " for " + workflowName + " %s";

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

        Transformation joiner = workflow.getTransformations().stream().filter(transformation ->
                        transformation.type().equals(Transformations.JOINER) &&
                                transformation.name().equals(transformationName))
                .toList().get(0);

        Map<String, Object> parameters = joiner.parameters();
        List<JoinSource> joinSources = mapper.map((List<?>) parameters.get("joinSources"), JoinSource.class);
        List<Map<String, String>> conditions = (List<Map<String, String>>) parameters.get("conditions");
        Map<String, String> finalStructure = (Map<String, String>) parameters.get("finalStructure");

        try (Connection etl = dataSource.getConnection()) {
            Statement etlStatement = etl.createStatement();

            String joinQuery = getJoinData(etlStatement, joinSources, conditions, transformationName) + ";";
            StringBuilder insertJoinIntoTable = new StringBuilder("INSERT INTO " + transformationName + " (");

            List<String> columns = joinSources.stream().flatMap(src -> src.getColumns().stream()).toList();
            columns.forEach(cln -> insertJoinIntoTable.append(cln).append(", "));

            insertJoinIntoTable.delete(insertJoinIntoTable.length() - 2, insertJoinIntoTable.length()).append("( ");
            insertJoinIntoTable.append(joinQuery);

            etlStatement.executeUpdate(insertJoinIntoTable.toString());

            renameColumns(etlStatement, finalStructure, transformationName);

            response = new TransformationResponse(workflowId,
                    workflowName,
                    transformationName,
                    String.format(responseString, "finished successfully!"),
                    null,
                    joinSources.stream().map(JoinSource::getName).toList());
        } catch (SQLException e) {
            response = new TransformationResponse(workflowId,
                    workflowName,
                    Transformations.SORTER.name(),
                    String.format(responseString, "failed. Workflow doesn't exist!"),
                    null,
                    null);

        }

        sendResponse(response);
    }

    private void renameColumns(Statement etlStatement, Map<String, String> finalStructure, String tableName) throws SQLException {
        ResultSet existingTable = etlStatement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0;");
        StringBuilder alterColumnsNames = new StringBuilder("ALTER TABLE " + tableName + " RENAME COLUMN ");

        Set<String> existingColumnsKeys = finalStructure.keySet();
        existingTable.next();

        for (String key : existingColumnsKeys) {
            alterColumnsNames.append(key).append(" TO ").append(finalStructure.get(key)).append(", ");
        }

        alterColumnsNames.delete(alterColumnsNames.length() - 2, alterColumnsNames.length()).append(";");
        etlStatement.executeUpdate(alterColumnsNames.toString());
    }

    private String getJoinData(Statement etlStatement, List<JoinSource> sources, List<Map<String, String>> conditions, String tableName) throws SQLException {
        StringBuilder selectQuery = new StringBuilder("SELECT ");
        StringBuilder joinQuery = new StringBuilder(" FROM ");

        for (int i = 0; i < sources.size(); i++) {
            JoinSource source = sources.get(i);
            List<String> columns = source.getColumns();
            String sourceName = source.getName();

            for (String column : columns) {
                selectQuery.append(sourceName).append('.').append(column).append(", ");
            }

            if (i == 0) {
                joinQuery.append(sourceName).append(" ");
            } else {
                JoinSource previousSource = sources.get(i - 1);
                String previousSourceName = previousSource.getName();
                Optional<Map<String, String>> optionalMap = conditions.stream().filter(condition -> condition.containsKey(sourceName) &&
                        condition.containsKey(previousSourceName)).findFirst();

                if (optionalMap.isPresent()) {
                    joinQuery.append(" INNER JOIN ").append(sourceName).append(" ON ");

                    Map<String, String> condition = optionalMap.get();

                    joinQuery.append(sourceName).append('.').append(condition.get(sourceName));
                    joinQuery.append(" = ");
                    joinQuery.append(previousSourceName).append('.').append(condition.get(previousSourceName));
                }
            }
        }

        selectQuery.delete(selectQuery.length() - 2, selectQuery.length()).append(" ");
        selectQuery.append(joinQuery);

        ResultSet joinResult = etlStatement.executeQuery(selectQuery + " WHERE 1 = 0;");
        ResultSetMetaData metadata = joinResult.getMetaData();

        createJoinTable(etlStatement, tableName, metadata, sources);

        return selectQuery.toString();
    }

    private void createJoinTable(Statement etlStatement, String tableName, ResultSetMetaData metadata, List<JoinSource> sources) throws SQLException {
        StringBuilder createTable = new StringBuilder("CREATE TABLE " + tableName + "(\nid SERIAL PRIMARY KEY, \n");

        List<String> columns = sources.stream().flatMap(source -> source.getColumns().stream()).toList();

        List<String> columnsTypes = new ArrayList<>();
        int columnCount = metadata.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            String columnDataType = metadata.getColumnTypeName(i + 1);
            int precision = metadata.getPrecision(i + 1);
            int scale = metadata.getScale(i + 1);

            if (!columnDataType.equals("serial") &&
                    !columnDataType.equals("bytea") &&
                    !columnDataType.equals("text") &&
                    !columnDataType.equals("json")) {
                columnDataType += "(" + precision;
                if (scale != 0) {
                    columnDataType += ", " + scale;
                }
                columnDataType += ")";
            }

            columnsTypes.add(i, columnDataType);
        }

        for (int i = 0; i < columnCount; i++) {
            String column = columns.get(i);
            String columnType = columnsTypes.get(i);

            createTable.append(column).append(" ").append(columnType);
            if (i != columnCount - 1) {
                createTable.append(", ");
            }

        }
        createTable.append(");");

        etlStatement.executeUpdate(createTable.toString());
    }

    private void sendResponse(TransformationResponse response) {
        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }
}
