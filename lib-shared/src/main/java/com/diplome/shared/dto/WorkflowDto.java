package com.diplome.shared.dto;

import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.Target;
import com.diplome.shared.elements.Transformation;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public class WorkflowDto {

    @JsonProperty("workflowName")
    private String workflowName;

    @JsonProperty("sources")
    private List<Source> sources;

    @JsonProperty("targets")
    private List<Target> targets;

    @JsonProperty("transformations")
    private List<Transformation> transformations;
}
