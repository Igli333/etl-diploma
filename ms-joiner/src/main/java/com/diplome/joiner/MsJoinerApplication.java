package com.diplome.joiner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication(scanBasePackages = {"com.diplome.shared.configuration", "com.diplome.joiner"})
@EnableMongoRepositories(basePackages = {"com.diplome.shared.repositories"})
public class MsJoinerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsJoinerApplication.class, args);
    }

}
