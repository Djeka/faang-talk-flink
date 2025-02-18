package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.model.*;
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

    protected <R extends AbstractRecord> DataStream<R> createSource(String topic, Class<R> clazz, int parallelism) {
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(pipelineConfig.getKafkaParams());

        KafkaSource<R> kafkaSource = KafkaSource.<R>builder()
                .setProperties(kafkaProps)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new MeterRecordDeserializer<R>(clazz))
                .build();

        return env.fromSource(kafkaSource, watermarkStrategy(), pipelineConfig.getMetersTopic())
                .name("source-from-" + pipelineConfig.getMetersTopic())
                .setParallelism(parallelism);
    }

    private <R extends AbstractRecord> WatermarkStrategy<R> watermarkStrategy() {
        return WatermarkStrategy.<R>forMonotonousTimestamps()
            .withIdleness(Duration.ofMinutes(1))
            .withTimestampAssigner((SerializableTimestampAssigner<R>) (record, kafkaTimestamp) -> {
                return record.getTimestamp();
            });
    }

    protected KafkaSink<MeterRecord> sink() {
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(pipelineConfig.getKafkaParams());

        return KafkaSink.<MeterRecord>builder()
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new MeterRecordSerializer(pipelineConfig.getSinkTopic()))
                .build();
    }
}
