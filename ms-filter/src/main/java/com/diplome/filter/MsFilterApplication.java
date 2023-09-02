package com.diplome.filter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication(scanBasePackages = {"com.diplome.shared.configuration", "com.diplome.filter"})
@EnableMongoRepositories(basePackages = {"com.diplome.shared.repositories"})
public class MsFilterApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsFilterApplication.class, args);
    }

}
