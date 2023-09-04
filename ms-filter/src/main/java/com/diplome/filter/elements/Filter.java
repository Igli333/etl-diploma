package com.diplome.filter.elements;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Service;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Filter {
    String logicalOperator;
    String filterColumn;
    String filterRule;
}
