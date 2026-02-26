package com.abdelwahab.api_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
/**
 * Application entry point for the API service.
 */
public class ApiServiceApplication {

	/**
	 * Bootstraps the Spring Boot application.
	 *
	 * @param args CLI arguments forwarded to Spring Boot
	 */
	public static void main(String[] args) {
		// Launch the Spring Boot context.
		SpringApplication.run(ApiServiceApplication.class, args);
	}

}
