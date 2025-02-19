package com.emolokov.faang_talk_flink;

import com.emolokov.faang_talk_flink.generator.PressRecordsGenerator;
import com.emolokov.faang_talk_flink.generator.TempRecordsGenerator;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


@Slf4j
public class StartEventsGenerators {
    protected static final ObjectMapper YAML_MAPPER = new YAMLMapper();

    @Test
    public void generateEvents() throws Exception {
        var pipelineConfig = PipelineConfig.load("test-pipeline-config.yaml");

        // start events generators
        Future<?> tempGenFuture = new TempRecordsGenerator(pipelineConfig).start();
        Future<?> pressGenFuture = new PressRecordsGenerator(pipelineConfig).start();


        tempGenFuture.get(1000, TimeUnit.SECONDS);
        pressGenFuture.get(1000, TimeUnit.SECONDS);
    }
}
