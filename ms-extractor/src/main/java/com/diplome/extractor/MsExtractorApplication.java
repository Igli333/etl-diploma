package com.diplome.extractor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
@EnableMongoRepositories(basePackages = {"com.diplome.shared.repositories"})
public class MsExtractorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsExtractorApplication.class, args);
    }

}
