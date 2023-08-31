package com.diplome.extractor.service;

import com.diplome.shared.elements.TransformationRequest;

public interface ExtractorService {
    void addDatabaseTableLocally(TransformationRequest transformationRequest);
}
