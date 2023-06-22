package com.diplome.shared.repositories;

import com.diplome.shared.entities.Workflow;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface WorkflowRepository extends MongoRepository<Workflow, Integer> {
}
