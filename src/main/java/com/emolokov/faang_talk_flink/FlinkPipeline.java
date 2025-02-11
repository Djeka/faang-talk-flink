package com.emolokov.faang_talk_flink;

import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.MeterRecordDeserializer;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

    protected DataStream<MeterRecord> createSource(){
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(pipelineConfig.getKafkaParams());

        KafkaSource<MeterRecord> kafkaSource = KafkaSource.<MeterRecord>builder()
                .setProperties(kafkaProps)
                .setTopics(pipelineConfig.getSourceTopic())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new MeterRecordDeserializer())
                .build();

        return env.fromSource(kafkaSource, watermarkStrategy(), pipelineConfig.getSourceTopic())
                .name("source-from-" + pipelineConfig.getSourceTopic())
                .setParallelism(1);
    }

    private WatermarkStrategy<MeterRecord> watermarkStrategy() {
        return WatermarkStrategy.<MeterRecord>forMonotonousTimestamps()
            .withIdleness(Duration.ofMinutes(1))
            .withTimestampAssigner((SerializableTimestampAssigner<MeterRecord>) (record, kafkaTimestamp) -> {
                return record.getTimestamp();
            });
    }
}
