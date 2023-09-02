package com.diplome.extractor.service.implementation;

import com.diplome.extractor.service.ExtractorService;
import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.DatabaseDrivers;
import com.diplome.shared.enums.Transformations;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@AllArgsConstructor
@Log4j2
public class ExtractorServiceImplementation implements ExtractorService {

    final private DataSource dataSource;
    final private WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;

    private Connection connectToSourceDatabase(Source source) throws SQLException, ClassNotFoundException {
        String databaseType = source.databaseType();
        String driver = DatabaseDrivers.databaseDrivers.get(databaseType.toUpperCase());
        Class.forName(driver);
        String URI = "jdbc:" + databaseType.toLowerCase() + "://" + source.URI().replace("www.", "");
        return DriverManager.getConnection(URI, source.username(), source.password());
    }

    @Override
    public void addDatabaseTableLocally(TransformationRequest transformationRequest) {
        String workflowId = transformationRequest.workFlowId();
        Workflow workflow = null;
        TransformationResponse response = null;
        if (workflowRepository.findById(workflowId).isPresent()) {
            workflow = workflowRepository.findById(workflowId).get();
        } else {
            response = new TransformationResponse(workflowId,
                    "", null,
                    "Extraction for \" + workflow.getWorkflowName() + \" source: \" +  source.name() + \" failed");
            kafkaTemplate.send(Transformations.RESPONSE.name(), response);
            return;
        }

        Source source = workflow.getSources().stream()
                .filter(src -> src.name().equals(transformationRequest.parameters().get("referenceSource"))).toList().get(0);


        try (Connection connection = connectToSourceDatabase(source)) {
            String tableName = source.tableName();

            Statement sqlCall = connection.createStatement();
            ResultSet getTable = sqlCall.executeQuery("SELECT * FROM " + tableName + ";");

            ResultSetMetaData resultSetMetaData = getTable.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            List<String> columns = new ArrayList<>();
            List<String> columnsTypes = new ArrayList<>();

            for (int i = 0; i < columnCount; i++) {
                columns.add(i, resultSetMetaData.getColumnName(i + 1));

                String columnDataType = resultSetMetaData.getColumnTypeName(i + 1);
                int precision = resultSetMetaData.getPrecision(i + 1);
                int scale = resultSetMetaData.getScale(i + 1);

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

            String createTableQuery = createTable(connection, tableName, columns, columnsTypes, columnCount);
            List<String> insertions = insertionQueries(columns, columnsTypes, getTable, tableName, columnCount);

            Connection etlDb = dataSource.getConnection();

            try (Statement localDBStatement = etlDb.createStatement()) {
                localDBStatement.executeUpdate(createTableQuery);

               for (String insertion : insertions) {
                    localDBStatement.executeUpdate(insertion);
                }
            } catch (Exception e) {
                log.log(Level.ERROR, e);
                response = new TransformationResponse(workflowId,
                        "", null,
                        "Extraction for \" + workflow.getWorkflowName() + \" source: \" +  source.name() + \" failed");
                kafkaTemplate.send(Transformations.RESPONSE.name(), response);
                return;
            }

            response = new TransformationResponse(workflowId, "",
                    "Extraction for " + workflow.getWorkflowName() + " source: " + source.name() + " finished",
                    null);

        } catch (SQLException | ClassNotFoundException e) {
            log.log(Level.ERROR, e);
            response = new TransformationResponse(workflowId,
                    "", null,
                    "Extraction for \" + workflow.getWorkflowName() + \" source: \" +  source.name() + \" failed");

        }

        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }

    private String createTable(Connection connection, String tableName, List<String> columns, List<String> columnsTypes, int columnCount) throws SQLException {
        StringBuilder createTable = new StringBuilder("CREATE TABLE " + tableName + " (");

        ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, tableName);
        primaryKeys.next();

        for (int i = 0; i < columnCount; i++) {
            String column = columns.get(i);
            String columnType = columnsTypes.get(i);

            if (Objects.equals(columnType, "LONGVARCHAR")) {
                columnType = "VARCHAR(255)";
            }

            createTable.append(column).append(" ").append(columnType);

            if (primaryKeys.getString("COLUMN_NAME").equals(columns.get(i))) {
                createTable.append(" ").append("PRIMARY KEY");
            }

            if (i != columnCount - 1) {
                createTable.append(", ");
            }

        }
        createTable.append(");");

        return createTable.toString();
    }

    private List<String> insertionQueries(List<String> columns, List<String> columnTypes, ResultSet getTable, String tableName, int columnCount) throws SQLException {
        List<String> insertions = new ArrayList<>();

        StringBuilder columnString = new StringBuilder();

        for (int i = 0; i < columnCount; i++) {
            columnString.append(columns.get(i));
            if (i != columnCount - 1) {
                columnString.append(", ");
            }
        }

        while (getTable.next()) {
            StringBuilder insertNewTable = new StringBuilder("INSERT INTO " + tableName + "(").append(columnString);
            insertNewTable.append(") VALUES(");

            for (int i = 0; i < columnCount; i++) {
                String newValue = getTable.getString(columns.get(i));
                if (columnTypes.get(i).startsWith("varchar") ||
                        columnTypes.get(i).startsWith("char") ||
                        columnTypes.get(i).startsWith("text") ||
                        columnTypes.get(i).startsWith("date") ||
                        columnTypes.get(i).startsWith("time") ||
                        columnTypes.get(i).startsWith("timestamp") ||
                        columnTypes.get(i).startsWith("array") ||
                        columnTypes.get(i).startsWith("json")) {

                    insertNewTable.append("'").append(newValue.replace("'", "''")).append("'");
                } else {
                    insertNewTable.append(newValue);
                }
                if (i != columnCount - 1) {
                    insertNewTable.append(", ");
                }

            }
            insertNewTable.append(");");
            insertions.add(insertNewTable.toString());
        }

        return insertions;
    }

}
