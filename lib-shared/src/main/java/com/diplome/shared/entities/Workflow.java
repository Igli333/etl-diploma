package com.diplome.shared.entities;

import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.Target;
import com.diplome.shared.elements.Transformation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Getter
@Setter
@Document(collection = "workflows")
@NoArgsConstructor
@AllArgsConstructor
public class Workflow {
    @Id
    private String id;
    private String workflowName;
    private Source source;
    private Target target;
    private List<Transformation> transformations;
}
