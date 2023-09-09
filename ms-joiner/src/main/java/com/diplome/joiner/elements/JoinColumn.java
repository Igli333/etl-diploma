package com.diplome.joiner.elements;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class JoinColumn {
    private String columnName;
    private String className;
    private String label;
    private int type;
    private String typeName;
    private int precision;
    private int scale;
}
