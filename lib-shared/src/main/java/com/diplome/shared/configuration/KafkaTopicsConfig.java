package com.diplome.shared.configuration;

import com.diplome.shared.enums.Transformations;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic extractor() {
        return new NewTopic(Transformations.EXTRACTOR.name(), 2, (short) 1);
    }

    @Bean
    public NewTopic filter() {
        return new NewTopic(Transformations.FILTER.name(), 2, (short) 1);
    }

    @Bean
    public NewTopic joiner() {
        return new NewTopic(Transformations.JOINER.name(), 2, (short) 1);
    }

    @Bean
    public NewTopic merger() {
        return new NewTopic(Transformations.MERGER.name(), 2, (short) 1);
    }

    @Bean
    public NewTopic sorter() {
        return new NewTopic(Transformations.SORTER.name(), 2, (short) 1);
    }

    @Bean
    public NewTopic loader() {
        return new NewTopic(Transformations.LOADER.name(), 2, (short) 1);
    }
}
