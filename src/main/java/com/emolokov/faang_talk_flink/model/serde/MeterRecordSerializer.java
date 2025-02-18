package com.emolokov.faang_talk_flink.model.serde;

import com.emolokov.faang_talk_flink.model.records.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class MeterRecordSerializer<R extends Record> implements KafkaRecordSerializationSchema<R> {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final String topic;

    public MeterRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(R record, KafkaSinkContext context, Long timestamp) {
        try {
            log.info("Sink record: {}", record);
            return new ProducerRecord<>(topic, JSON_MAPPER.writeValueAsBytes(record));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
