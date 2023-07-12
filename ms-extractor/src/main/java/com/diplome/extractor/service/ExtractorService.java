package com.diplome.extractor.service;

import com.diplome.shared.elements.Source;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.DatabaseDrivers;
import com.diplome.shared.repositories.WorkflowRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class ExtractorService {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private WorkflowRepository workflowRepository;


    private Connection connectToSourceDatabase(Source source) throws SQLException, ClassNotFoundException {
        String databaseType = source.databaseType();
        String driver = DatabaseDrivers.databaseDrivers.get(databaseType);
        Class.forName(driver);
        String URI = "jdbc:" + databaseType.toLowerCase() + "//" + source.URI().replace("www.", "");
        return DriverManager.getConnection(URI, source.username(), source.password());
    }

    public void addDatabaseTableLocally(Source source) throws SQLException, ClassNotFoundException {
        try (Connection connection = connectToSourceDatabase(source)) {
            String tableName = source.tableName();
            String useDatabase = "USE DATABASE " + source.name() + ";";
            String getTable = "SELECT * FROM " + tableName + ";";

            Statement sqlCall = connection.createStatement();
            ResultSet useDb = sqlCall.executeQuery(useDatabase);
            ResultSet getTbl = sqlCall.executeQuery(getTable);

            // Creating table in local repository database ...


            ResultSetMetaData resultSetMetaData = getTbl.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<String> columns = new ArrayList<>(columnCount);
            List<String> columnsTypes = new ArrayList<>(columnCount);

            for (int i = 1; i <= columnCount; i++) {
                columns.add(i, resultSetMetaData.getColumnClassName(i));
                columnsTypes.add(i, resultSetMetaData.getColumnTypeName(i));
            }

            StringBuilder createTable = new StringBuilder("CREATE TABLE " + tableName + " (");

            int i = 0;
            for (String column : columns){
                createTable.append(column).append(" ").append(columnsTypes.get(i)).append(", ");
                i++;
            }

            createTable.append(");");

            StringBuilder insertNewTable = new StringBuilder("INSERT INTO " + source.tableName() + "(");

            for (String column : columns) {
                insertNewTable.append(column).append(", ");
            }

            insertNewTable.append(") VALUES(");


            Connection etlDb = dataSource.getConnection();
        }
    }

    public Source getWorkflowInformation(Integer id) {
        Workflow wf = workflowRepository.findWorkflowById(id);
        return wf.getSource();
    }
}
