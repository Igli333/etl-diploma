package com.diplome.extractor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class MsExtractorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsExtractorApplication.class, args);
    }

}
