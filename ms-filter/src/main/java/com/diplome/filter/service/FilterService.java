package com.diplome.filter.service;

import com.diplome.shared.elements.TransformationRequest;

import java.sql.SQLException;

public interface FilterService {
    void filter(TransformationRequest request);
}
