package com.emolokov.faang_talk_flink.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class MeterRecordDeserializer<R extends AbstractRecord> implements KafkaRecordDeserializationSchema<R> {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Class<R> clazz;

    public MeterRecordDeserializer(Class<R> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumedRecord, Collector<R> out) throws IOException {
        if(consumedRecord.value() == null) return; // ignore null values

        R record = JSON_MAPPER.readValue(consumedRecord.value(), clazz);
        out.collect(record);
    }

    @Override
    public TypeInformation<R> getProducedType() {
        return TypeInformation.of(clazz);
    }
}
