package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipelineTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;


@Slf4j
public class JoinPressPipelineTest extends FlinkPipelineTest {
    protected FlinkPipeline createPipeline() throws IOException {
        return new JoinPressPipeline(this.pipelineConfig, prepareFlinkEnv());
    }

    @Test
    public void testPipeline() throws Exception {
        this.pipeline.run();
    }
}
