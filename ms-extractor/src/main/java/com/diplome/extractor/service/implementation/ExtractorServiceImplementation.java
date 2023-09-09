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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
@Log4j2
public class ExtractorServiceImplementation implements ExtractorService {

    final private DataSource dataSource;
    final private WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, TransformationResponse> kafkaTemplate;

    @Override
    @Transactional
    public void addDatabaseTableLocally(TransformationRequest transformationRequest) {
        String workflowId = transformationRequest.workflowId();
        String workflowName = transformationRequest.workflowName();
        String referenceSource = transformationRequest.referenceSource();

        Workflow workflow;
        TransformationResponse response;
        String responseString = "Extraction for workflow: " + workflowName + " source: " + referenceSource + " %s";

        if (workflowRepository.findById(workflowId).isPresent()) {
            workflow = workflowRepository.findById(workflowId).get();
        } else {
            response = new TransformationResponse(workflowId,
                    workflowName,
                    null,
                    String.format(responseString, "failed. Workflow doesn't exist!"),
                    null,
                    null);
            sendResponse(response);
            return;
        }

        Source source = workflow.getSources().stream()
                .filter(src -> src.name().equals(referenceSource)).toList().get(0);

        try (Connection connection = connectToSourceDatabase(source)) {
            String tableName = source.tableName();

            Statement sqlCall = connection.createStatement();
            ResultSet getTable = sqlCall.executeQuery("SELECT * FROM " + tableName + ";");

            ResultSetMetaData resultSetMetaData = getTable.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            List<String> columns = new ArrayList<>();
            List<String> columnsTypes = new ArrayList<>();

            getColumnInformation(columns, columnsTypes, columnCount, resultSetMetaData);

            String createTableQuery = createTable(connection, referenceSource, columns, columnsTypes, columnCount, tableName);
            List<String> insertions = insertionQueries(columns, columnsTypes, getTable, referenceSource, columnCount);

            Connection etlDb = dataSource.getConnection();

            Statement localDBStatement = etlDb.createStatement();
            localDBStatement.executeUpdate(createTableQuery);

            for (String insertion : insertions) {
                localDBStatement.executeUpdate(insertion);
            }

            response = new TransformationResponse(workflowId,
                    workflowName,
                    "Extraction",
                    String.format(responseString, "finished!"),
                    null,
                    List.of(referenceSource));


        } catch (SQLException | ClassNotFoundException e) {
            log.log(Level.ERROR, e);
            response = new TransformationResponse(workflowId,
                    workflowName,
                    workflowId + "-" + referenceSource,
                    null,
                    String.format(responseString, "failed."),
                    null);
        }

        sendResponse(response);
    }

    private void getColumnInformation(List<String> columns, List<String> columnsTypes, int columnCount, ResultSetMetaData resultSetMetaData) throws SQLException {
        for (int i = 0; i < columnCount; i++) {
            columns.add(i, resultSetMetaData.getColumnName(i + 1));

            String columnDataType = resultSetMetaData.getColumnTypeName(i + 1);
            int precision = resultSetMetaData.getPrecision(i + 1);
            int scale = resultSetMetaData.getScale(i + 1);

            if (!columnDataType.equals("serial") &&
                    !columnDataType.equals("bytea") &&
                    !columnDataType.equals("text") &&
                    !columnDataType.equals("json") &&
                    !columnDataType.equals("date")) {
                columnDataType += "(" + precision;
                if (scale != 0) {
                    columnDataType += ", " + scale;
                }
                columnDataType += ")";
            }

            columnsTypes.add(i, columnDataType);
        }
    }

    private Connection connectToSourceDatabase(Source source) throws SQLException, ClassNotFoundException {
        String databaseType = source.databaseType();
        String driver = DatabaseDrivers.databaseDrivers.get(databaseType.toUpperCase());
        Class.forName(driver);
        String URI = "jdbc:" + databaseType.toLowerCase() + "://" + source.URI().replace("www.", "");
        return DriverManager.getConnection(URI, source.username(), source.password());
    }

    private String createTable(Connection connection, String referenceSource, List<String> columns, List<String> columnsTypes, int columnCount, String tableName) throws SQLException {
        StringBuilder createTable = new StringBuilder("CREATE TABLE " + referenceSource + " (");

        ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, connection.getSchema(), tableName);
        primaryKeys.next();
        String primaryKey = primaryKeys.getString("COLUMN_NAME");

        for (int i = 0; i < columnCount; i++) {
            String column = columns.get(i);
            String columnType = columnsTypes.get(i);

            createTable.append(column).append(" ").append(columnType);

            if (primaryKey.equals(column)) {
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

    private void sendResponse(TransformationResponse response) {
        kafkaTemplate.send(Transformations.RESPONSE.name(), response);
    }
}
