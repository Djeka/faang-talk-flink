package com.emolokov.faang_talk_flink.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class MeterRecordDeserializer implements KafkaRecordDeserializationSchema<MeterRecord> {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumedRecord, Collector<MeterRecord> out) throws IOException {
        if(consumedRecord.value() == null) return; // ignore null values

        MeterRecord meterRecord = JSON_MAPPER.readValue(consumedRecord.value(), MeterRecord.class);
        out.collect(meterRecord);
    }

    @Override
    public TypeInformation<MeterRecord> getProducedType() {
        return TypeInformation.of(MeterRecord.class);
    }
}
