package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.FlinkPipeline;
import com.emolokov.faang_talk_flink.model.MeterRecord;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

import static com.emolokov.faang_talk_flink.model.MeterRecord.TempUnit.K;


@Slf4j
public class AvgTempPipelineTest extends AbstractPipelineTest {
    @Override
    protected double messagesPerSec() {
        return 1;
    }

    @Override
    protected Supplier<MeterRecord> meterRecordSupplier() {
        return () -> new MeterRecord(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                K,
                20d + 10d * (RANDOM.nextDouble() - 0.5)
        );
    }

    @Override
    protected String pipelineConfigFile() {
        return "avg-temp-pipeline.config.yaml";
    }

    protected FlinkPipeline createPipeline() throws IOException {
        return new AvgTempPipeline(this.pipelineConfig, prepareFlinkEnv());
    }

    @Test
    public void testPipeline() throws Exception {
        produceTestEvents();

        this.pipeline.run();
    }
}
