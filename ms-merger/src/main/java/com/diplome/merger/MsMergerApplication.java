package com.diplome.merger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication(scanBasePackages = {"com.diplome.shared.configuration", "com.diplome.merger"})
@EnableMongoRepositories(basePackages = {"com.diplome.shared.repositories"})
public class MsMergerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsMergerApplication.class, args);
    }

}
