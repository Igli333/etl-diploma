package com.diplome.shared.configuration;

import org.modelmapper.ModelMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DtoEntityMapper {
    @Bean
    ModelMapper modelMapper(){
        return new ModelMapper();
    }
}
