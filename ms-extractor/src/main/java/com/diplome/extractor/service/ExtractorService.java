package com.diplome.extractor.service;

import com.diplome.shared.elements.Source;
import com.diplome.shared.entities.Workflow;
import com.diplome.shared.enums.DatabaseDrivers;
import com.diplome.shared.repositories.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;

@Service
@RequiredArgsConstructor
public class ExtractorService {

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
            String useDatabase = "USE DATABASE " + source.name() + ";";
            String getTable = "SELECT * FROM " + source.tableName() + ";";

            Statement sqlCall = connection.createStatement();
            ResultSet useDb = sqlCall.executeQuery(useDatabase);
            ResultSet getTbl = sqlCall.executeQuery(getTable);

            // Creating table in local repository database ...
        }
    }

    public Source getWorkflowInformation(Integer id) {
        Workflow wf = workflowRepository.findWorkflowById(id);
        return wf.getSource();
    }
}
