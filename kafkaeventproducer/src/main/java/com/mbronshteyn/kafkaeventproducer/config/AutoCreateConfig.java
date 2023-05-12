package com.mbronshteyn.kafkaeventproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class AutoCreateConfig {

    @Bean
    NewTopic libraryEvents() {
        // we are running on a local cluster with only one broker
        // thus put replicas count to 1. in prod should be at least 3
        return TopicBuilder.name("library-events")
                .partitions(8)
                .replicas(1)
                .build();
    }
}
