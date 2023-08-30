package com.diplome.shared.configuration;

import jakarta.annotation.Resource;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

@Configuration
public class SharedDatabase {
    @Resource
    private Environment environment;

    @Bean
    public DataSource etlDatabase(){
        DataSourceBuilder<?> dataSourceBuilder =  DataSourceBuilder.create();
        return dataSourceBuilder.build();
    }

}
