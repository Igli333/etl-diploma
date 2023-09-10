package com.diplome.shared.elements;

import org.springframework.data.mongodb.core.index.Indexed;

public record Target(@Indexed(unique = true) String name,
                     String databaseType,
                     String URI,
                     String username,
                     String password,
                     String reference) {
}
