package com.marcosfurquim.iotcase.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "case")
public record CaseProperties(String csvPath, Topics topics, int partitions) {
    public record Topics(String events, String deadLetter) {}
}
