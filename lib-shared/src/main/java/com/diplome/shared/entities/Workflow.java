package com.diplome.shared.entities;

import com.diplome.shared.elements.Source;
import com.diplome.shared.elements.Target;
import com.diplome.shared.elements.Transformation;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Getter
@Setter
@Document(collection = "workflows")
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Workflow {
    @Id
    private String id;

    @Field("workflowName")
    private String workflowName;

    @Field("source")
    private Source source;

    @Field("target")
    private Target target;

    @Field("transformations")
    private List<Transformation> transformations;
}
