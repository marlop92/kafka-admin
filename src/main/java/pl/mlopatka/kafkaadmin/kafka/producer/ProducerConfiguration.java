package pl.mlopatka.kafkaadmin.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
public class ProducerConfiguration {

    @Bean
    public Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

    @Bean("stringProducerFactory")
    public ProducerFactory<String, String> stringProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean("byteProducerFactory")
    public ProducerFactory<String, byte[]> byteProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean("stringTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(stringProducerFactory());
    }

    @Bean("byteTemplate")
    public KafkaTemplate<String, byte[]> bytesTemplate() {
        return new KafkaTemplate<>(byteProducerFactory(),
                Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @PostConstruct
    private void onStartup() {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate()
                .send("tmp-topic", "something");

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Failed to send a message!");
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sucessfully sent a message!");
            }
        });

        ListenableFuture<SendResult<String, String>> nextFuture = kafkaTemplate()
                .send("tmp-topic", "something");

        future.addCallback(new KafkaSendCallback<>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sucessfully sent a message!");
            }

            @Override
            public void onFailure(KafkaProducerException ex) {
                ProducerRecord<Integer, String> failed = ex.getFailedProducerRecord();
                System.out.println(failed.key() + " " + ex.getMessage());
            }

        });

        sendToKafka();
    }

    public void sendToKafka() {
        final ProducerRecord<String, String> record = new ProducerRecord<>("tmp-topic",
                "key", "value");


        try {
            kafkaTemplate().send(record).get(10, TimeUnit.SECONDS);
            kafkaTemplate().flush();
            System.out.println("Succesfully sent!");
//            handleSuccess(data);
        }
        catch (ExecutionException e) {
//            handleFailure(data, record, e.getCause());
        }
        catch (TimeoutException | InterruptedException e) {
//            handleFailure(data, record, e);
        }
    }
}
