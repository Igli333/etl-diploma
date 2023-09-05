package com.diplome.sorter.service.implementation;

import com.diplome.shared.configuration.Mapper;
import com.diplome.shared.elements.Transformation;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import com.diplome.sorter.service.SorterService;
import com.diplome.sorter.elements.SortingRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;

@Service
@Log4j2
@RequiredArgsConstructor
public class SorterServiceImplementation implements SorterService {

    private final WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;
    private final DataSource dataSource;
    private final Mapper mapper;

    @Override
    @Transactional
    public void sort(TransformationRequest request) {
        String workflowId = request.workflowId();
        String workflowName = request.workflowName();
        String referenceSource = request.referenceSource();
        String transformationName = request.transformationName();

        Workflow workflow;
        TransformationResponse response;
        String responseString = "Sorter transformation " + transformationName + " for " + workflowName + " source: " + referenceSource + " %s";

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

        Transformation sorter = workflow.getTransformations().stream().filter(transformation ->
                        transformation.type().equals(Transformations.SORTER)
                                && transformation.name().equals(transformationName)
                                && transformation.parameters().get("source").equals(referenceSource))
                .toList().get(0);

        String createNewTableQuery = "CREATE TABLE " + referenceSource + "Copy (LIKE " + referenceSource + ");";

        try (Connection etl = dataSource.getConnection()) {
            Statement etlStatement = etl.createStatement();
            etlStatement.executeUpdate(createNewTableQuery);
            ResultSet columns = etlStatement.executeQuery("SELECT * FROM " + referenceSource + "Copy WHERE 1 = 0;");

            String transferQuery = transferQuery(etl, etlStatement, referenceSource, columns, sorter);
            etlStatement.executeUpdate(transferQuery);

            etlStatement.executeUpdate("DROP TABLE " + referenceSource);
            etlStatement.executeUpdate("ALTER TABLE " + referenceSource + "Copy RENAME TO " + referenceSource + ";");

            response = new TransformationResponse(workflowId,
                    workflowName,
                    transformationName,
                    String.format(responseString, "finished successfully!"),
                    null,
                    List.of(referenceSource));


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

    private String transferQuery(Connection connection, Statement statement, String tableName, ResultSet columnsResultSet, Transformation sorter) throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO " + tableName + "Copy (");
        StringBuilder select = new StringBuilder("SELECT ");
        ResultSetMetaData columnsMetadata = columnsResultSet.getMetaData();
        int numberOfColumns = columnsMetadata.getColumnCount();

        ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, tableName);
        primaryKeys.next();
        String primaryKey = primaryKeys.getString("COLUMN_NAME");

        fixPrimaryKey(statement, tableName, primaryKey);

        for (int i = 0; i < numberOfColumns; i++) {
            String columnName = columnsMetadata.getColumnName(i + 1);
            if (!primaryKey.equals(columnName)) {
                insert.append(columnName);
                select.append(columnName);
                if (i != numberOfColumns - 1) {
                    insert.append(", ");
                    select.append(", ");
                }
            }
        }

        insert.append(") ");
        select.append(" FROM ").append(tableName).append(" ORDER BY ").append(sortingCondition(sorter)).append(";");
        insert.append(select);

        return insert.toString();
    }

    private void fixPrimaryKey(Statement statement, String tableName, String primaryKey) throws SQLException {
        String createSequence = "CREATE SEQUENCE increment_seq START 1 INCREMENT 1;";
        statement.executeUpdate(createSequence);

        String alterSequence = "ALTER TABLE " + tableName + "Copy ALTER COLUMN " + primaryKey + " SET DEFAULT nextval('increment_seq');";
        statement.executeUpdate(alterSequence);

        String alterTableQuery = "ALTER TABLE " + tableName + "Copy ADD CONSTRAINT " + tableName + "Copy_pkey PRIMARY KEY (" + primaryKey + ");";
        statement.executeUpdate(alterTableQuery);
    }

    private String sortingCondition(Transformation sorter) {
        List<SortingRule> sortingRules = mapper.map((List<?>) sorter.parameters().get("sortingRules"), SortingRule.class);
        int rulesSize = sortingRules.size();

        StringBuilder rulesString = new StringBuilder();

        for (int i = 0; i < rulesSize; i++) {
            SortingRule rule = sortingRules.get(i);
            rulesString.append(rule.getSortColumn()).append(" ").append(rule.getOrder());
            if (i != rulesSize - 1) {
                rulesString.append(", ");
            }
        }

        return rulesString.toString();
    }

    private void sendResponse(TransformationResponse response) {
        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }
}
