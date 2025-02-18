package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.MeterRecord;
import com.emolokov.faang_talk_flink.model.records.Record;
import com.emolokov.faang_talk_flink.model.serde.MeterRecordDeserializer;
import com.emolokov.faang_talk_flink.model.serde.MeterRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Properties;

@AllArgsConstructor
@Getter
@Slf4j
public abstract class FlinkPipeline {
    protected final PipelineConfig pipelineConfig;
    protected final StreamExecutionEnvironment env;

    public void run(){
        buildFlinkPipeline();

        try {
            env.setParallelism(pipelineConfig.getParallelism());
            if(pipelineConfig.isDisableOperatorsChain()){
                env.disableOperatorChaining();
            }
            env.execute();
        } catch (Exception e) {
            log.error("Failed to execute Flink job");
            throw new RuntimeException(e);
        }
    }

    public void close() throws Exception {
        env.close();
    }

    protected abstract void buildFlinkPipeline();

    protected <R extends MeterRecord> DataStream<R> createSource(String topic, Class<R> clazz, int parallelism) {
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(pipelineConfig.getKafkaParams());

        KafkaSource<R> kafkaSource = KafkaSource.<R>builder()
                .setProperties(kafkaProps)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new MeterRecordDeserializer<R>(clazz))
                .build();

        return env.fromSource(kafkaSource, watermarkStrategy(), topic)
                .name("source-from-" + topic)
                .setParallelism(parallelism);
    }

    private <R extends MeterRecord> WatermarkStrategy<R> watermarkStrategy() {
        return WatermarkStrategy.<R>forMonotonousTimestamps()
            .withIdleness(Duration.ofMinutes(1))
            .withTimestampAssigner((SerializableTimestampAssigner<R>) (record, kafkaTimestamp) -> {
                return record.getEventTimestamp();
            });
    }

    protected <R extends Record> KafkaSink<R> sink() {
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(pipelineConfig.getKafkaParams());

        return KafkaSink.<R>builder()
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new MeterRecordSerializer<R>(pipelineConfig.getSinkTopic()))
                .build();
    }


}
