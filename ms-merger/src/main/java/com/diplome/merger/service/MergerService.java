package com.diplome.merger.service;

import com.diplome.shared.elements.TransformationRequest;

public interface MergerService {
    void merge(TransformationRequest... request);
}
