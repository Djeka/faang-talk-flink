package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.pipelines.FlinkPipelineTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;


@Slf4j
public class AlignTempPipelineTest extends FlinkPipelineTest {

    @Test
    public void testPipeline() throws Exception {
        new AlignTempPipeline(getPipelineConfig(), env()).run();
    }
}
