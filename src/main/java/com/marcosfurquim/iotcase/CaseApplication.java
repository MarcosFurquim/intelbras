package com.marcosfurquim.iotcase;

import com.marcosfurquim.iotcase.config.CaseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(CaseProperties.class)
public class CaseApplication {
    public static void main(String[] args) {
        SpringApplication.run(CaseApplication.class, args);
    }
}
