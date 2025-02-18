package com.emolokov.faang_talk_flink.generator;

import com.emolokov.faang_talk_flink.model.AbstractRecord;
import com.google.common.util.concurrent.RateLimiter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor
public abstract class EventsGenerator<R extends AbstractRecord> {
    protected static final SecureRandom RANDOM = new SecureRandom();
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final double generateRatePerSecond;
    private final String brokerBootstrapServers;
    private String topic;
    private final Class<R> eventClass;
    private final Supplier<List<R>> eventsSupplier;

    public void start(){
        Producer<Bytes, Bytes> producer = newProducer();

        RateLimiter produceLimiter = RateLimiter.create(generateRatePerSecond);
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true){
                produceLimiter.acquire();

                List<R> records = eventsSupplier.get();

                try {
                    records.forEach(record -> {
                        produceToKafka(producer,
                                record.getId(),
                                JSON_MAPPER.valueToTree(record),
                                Map.of()
                        );
                    });
                } catch (Throwable t) {
                    log.error("Failed to produce test event", t);
                    throw new RuntimeException(t);
                }
            }
        });
    }

    private void produceToKafka(Producer<Bytes, Bytes> producer,
                                String meterId,
                                ObjectNode value,
                                Map<String, String> headers)  {
        try {
            ProducerRecord<Bytes, Bytes> producerRecord = new ProducerRecord<>(this.topic,
                    Bytes.wrap(meterId.getBytes()),
                    Bytes.wrap(JSON_MAPPER.writeValueAsBytes(value)));
            if(headers != null){
                headers.forEach((headerName, headerValue) -> producerRecord.headers().add(headerName, headerValue.getBytes()));
            }

            Future<RecordMetadata> future = producer.send(producerRecord);
            RecordMetadata recordMetadata = future.get(1L, TimeUnit.SECONDS);
            log.info("Produced to {}/{}: {}",
                    recordMetadata.topic(),
                    recordMetadata.offset(),
                    value.toString()
            );
        } catch (Exception e){
            throw new RuntimeException("Failed to produce to Kafka", e);
        }
    }

    private Producer<Bytes, Bytes> newProducer() {
        Properties configs = new Properties();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.brokerBootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        return new KafkaProducer<>(configs);
    }
}
