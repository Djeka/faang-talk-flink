package com.emolokov.faang_talk_flink.generator;

import com.emolokov.faang_talk_flink.model.records.MeterRecord;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@AllArgsConstructor
public abstract class EventsGenerator<R extends MeterRecord> {
    protected static final SecureRandom RANDOM = new SecureRandom();
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // generate meters templates that are used for meters records
    public static final List<Integer> LOCATIONS = IntStream.range(0, 3)
            .mapToObj(i -> i + 1)
            .collect(Collectors.toList());

    private final double generateRatePerSecond;
    private final String brokerBootstrapServers;
    private String topic;
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
                                record.getMeterId(),
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
            log.info("Produced to {}:{}:{} = {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
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
