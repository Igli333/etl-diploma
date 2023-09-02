package com.diplome.shared.configuration;

import com.diplome.shared.elements.TransformationRequest;
import com.diplome.shared.elements.TransformationResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    ConsumerFactory<String, TransformationRequest> consumerRequestFactory() {
        return new DefaultKafkaConsumerFactory<>(configProps());
    }

    ConsumerFactory<String, TransformationResponse> consumerResponseFactory() {
        return new DefaultKafkaConsumerFactory<>(configProps());
    }

    private Map<String, Object> configProps() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.diplome.shared.elements");
        return configProps;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TransformationRequest> kafkaListenerTransformationFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TransformationRequest> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerRequestFactory());
        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TransformationResponse> kafkaListenerCoordinatorFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TransformationResponse> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerResponseFactory());
        return factory;
    }
}
