package com.diplome.loader.service;

import com.diplome.shared.elements.TransformationRequest;

public interface LoaderService {
    void loadDatabaseRemotely(TransformationRequest transformationRequest);
}
