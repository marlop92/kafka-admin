package pl.mlopatka.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setFatalIfBrokerNotAvailable(true);

        return admin;
    }

    @Bean
    public NewTopic exampleTopic() {
        return TopicBuilder.name("example-topic")
                .partitions(10)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic tmpTopic() {
        return TopicBuilder.name("tmp-topic")
                .partitions(10)
                .replicas(1)
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }

    @Bean
    public NewTopic replicaTopic() {
        return TopicBuilder.name("replica-topic")
                .assignReplicas(0, Arrays.asList(0, 1))
                .assignReplicas(1, Arrays.asList(0, 1))
                .assignReplicas(2, Arrays.asList(0, 1))
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }

    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("nested-topic1")
                        .build(),
                TopicBuilder.name("nested-topic2")
                        .build()
        );
    }
}
