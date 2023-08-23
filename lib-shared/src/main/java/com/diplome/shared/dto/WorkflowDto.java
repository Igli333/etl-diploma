package com.diplome.shared.dto;

import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.Target;
import com.diplome.shared.elements.Transformation;

import java.util.List;

public class WorkflowDto {
    private String workflowName;
    private Source source;
    private Target target;
    private List<Transformation> transformations;
}
