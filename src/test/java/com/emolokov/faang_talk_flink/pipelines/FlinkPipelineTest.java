package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.generator.PressRecordsGenerator;
import com.emolokov.faang_talk_flink.generator.TempRecordsGenerator;
import com.emolokov.faang_talk_flink.http.EnrichmentHttpService;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class FlinkPipelineTest {
    protected static final ObjectMapper YAML_MAPPER = new YAMLMapper();

    @Getter
    private PipelineConfig pipelineConfig;
    private EnrichmentHttpService enrichmentService;

    @BeforeEach
    public void beforeTest() throws IOException, InterruptedException {
        this.enrichmentService = new EnrichmentHttpService();
        this.enrichmentService.start();
        log.info("Mock Http Service is started");

        this.pipelineConfig = PipelineConfig.load("test-pipeline-config.yaml");

        // start events generators
        new TempRecordsGenerator(pipelineConfig).start();
        new PressRecordsGenerator(pipelineConfig).start();
    }

    public StreamExecutionEnvironment env(String pipelineName) {
        Map<String, String> config = pipelineConfig.getFlinkConfig().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        config.put("metrics.reporter.grph.prefix", "faang." + pipelineName);

        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration.fromMap(config));
    }

    @AfterEach
    public void afterTest() throws Exception {
        this.enrichmentService.stop();
        log.info("Mock Http Service is stopped");
    }
}
