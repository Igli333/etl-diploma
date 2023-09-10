package com.diplome.loader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication(scanBasePackages = {"com.diplome.shared.configuration", "com.diplome.loader"})
@EnableMongoRepositories(basePackages = {"com.diplome.shared.repositories"})
public class MsLoaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsLoaderApplication.class, args);
    }

}
