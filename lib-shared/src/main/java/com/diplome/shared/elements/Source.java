package com.diplome.shared.elements;

import org.springframework.data.mongodb.core.index.Indexed;

public record Source(@Indexed(unique = true) String name,
                     String databaseType,
                     String URI,
                     String username,
                     String password,
                     String tableName) {

}
