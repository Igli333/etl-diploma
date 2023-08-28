package com.diplome.shared.dto;

import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.Target;
import com.diplome.shared.elements.Transformation;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public class WorkflowDto {

    @JsonProperty("workflowName")
    private String workflowName;

    @JsonProperty("source")
    private Source source;

    @JsonProperty("target")
    private Target target;

    @JsonProperty("transformations")
    private List<Transformation> transformations;
}
