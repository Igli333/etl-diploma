package com.diplome.extractor.service.implementation;

import com.diplome.extractor.service.ExtractorService;
import com.diplome.shared.elements.Source;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.DatabaseDrivers;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class ExtractorServiceImplementation implements ExtractorService {

    final private DataSource dataSource;
    final private WorkflowRepository workflowRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private Connection connectToSourceDatabase(Source source) throws SQLException, ClassNotFoundException {
        String databaseType = source.databaseType();
        String driver = DatabaseDrivers.databaseDrivers.get(databaseType);
        Class.forName(driver);
        String URI = "jdbc:" + databaseType.toLowerCase() + "//" + source.URI().replace("www.", "");
        return DriverManager.getConnection(URI, source.username(), source.password());
    }

    public void addDatabaseTableLocally(Integer workflowId) {
        Source source = getWorkflowInformation(workflowId);
        try (Connection connection = connectToSourceDatabase(source)) {
            String tableName = source.tableName();

            Statement sqlCall = connection.createStatement();
            sqlCall.executeQuery("USE DATABASE " + source.name() + ";");
            ResultSet getTable = sqlCall.executeQuery("SELECT * FROM " + tableName + ";");

            ResultSetMetaData resultSetMetaData = getTable.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            List<String> columns = new ArrayList<>(columnCount);
            List<String> columnsTypes = new ArrayList<>(columnCount);

            for (int i = 1; i <= columnCount; i++) {
                columns.add(i, resultSetMetaData.getColumnClassName(i));
                columnsTypes.add(i, resultSetMetaData.getColumnTypeName(i));
            }

            String createTableQuery = createTable(connection, tableName, columns, columnsTypes, columnCount);
            List<String> insertions = insertionQueries(columns, getTable, tableName, columnCount);

            Connection etlDb = dataSource.getConnection();

            try (Statement localDBStatement = etlDb.createStatement()) {
                localDBStatement.executeQuery(createTableQuery);

                for (String insertion : insertions) {
                    localDBStatement.executeQuery(insertion);
                }
            }

            kafkaTemplate.send("workflow", workflowId.toString());
        } catch (SQLException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private String createTable(Connection connection, String tableName, List<String> columns, List<String> columnsTypes, int columnCount) throws SQLException {
        StringBuilder createTable = new StringBuilder("CREATE TABLE " + tableName + " (");

        ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, tableName);
        primaryKeys.next();

        for (int i = 1; i <= columnCount; i++) {
            String column = columns.get(i);
            createTable.append(column).append(" ").append(columnsTypes.get(i));

            if (primaryKeys.getString("COLUMN_NAME").equals(columns.get(i))) {
                createTable.append("PRIMARY KEY");
            }

            createTable.append(", ");
        }
        createTable.append(");");

        return createTable.toString();
    }

    private List<String> insertionQueries(List<String> columns, ResultSet getTable, String tableName, int columnCount) throws SQLException {
        List<String> insertions = new ArrayList<>();

        StringBuilder columnString = new StringBuilder();

        for (String column : columns) {
            columnString.append(column).append(", ");
        }

        while (getTable.next()) {
            StringBuilder insertNewTable = new StringBuilder("INSERT INTO " + tableName + "(").append(columnString);
            insertNewTable.append(") VALUES(");

            for (int i = 1; i <= columnCount; i++) {
                insertNewTable.append(getTable.getObject(i)).append(", ");
            }
            insertNewTable.append(");");
            insertions.add(insertNewTable.toString());
        }

        return insertions;
    }

    private Source getWorkflowInformation(Integer id) {
        Workflow wf = workflowRepository.findWorkflowById(id);
        return wf.getSource();
    }
}