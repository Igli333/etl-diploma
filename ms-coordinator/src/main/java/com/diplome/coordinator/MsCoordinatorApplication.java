package com.diplome.coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication(scanBasePackages={"com.diplome.shared.configuration"})
@EnableMongoRepositories(basePackages = {"com.diplome.shared.repositories"})
public class MsCoordinatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsCoordinatorApplication.class, args);
    }

}
