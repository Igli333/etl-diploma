package com.diplome.shared.repositories;

import com.diplome.shared.entities.Workflow;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

public interface WorkflowRepository extends MongoRepository<Workflow, String> {
    @Query("{'_id': ?0}")
    Workflow findWorkflowById(String id);
}
