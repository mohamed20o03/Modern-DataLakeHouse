package com.abdelwahab.api_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Jackson configuration for ObjectMapper bean.
 */
@Configuration
public class JacksonConfig {

    /**
     * Provide a default ObjectMapper for JSON serialization.
     *
     * @return configured ObjectMapper instance
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
