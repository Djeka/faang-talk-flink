package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.generator.PressRecordsGenerator;
import com.emolokov.faang_talk_flink.generator.TempRecordsGenerator;
import com.emolokov.faang_talk_flink.http.EnrichmentHttpService;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

@Slf4j
public abstract class FlinkPipelineTest {
    protected static final ObjectMapper YAML_MAPPER = new YAMLMapper();

    private EnrichmentHttpService enrichmentService;
    protected PipelineConfig pipelineConfig;
    protected FlinkPipeline pipeline;

    protected abstract FlinkPipeline createPipeline() throws IOException;

    @BeforeEach
    public void beforeTest() throws IOException, InterruptedException {
        this.enrichmentService = new EnrichmentHttpService();
        this.enrichmentService.start();
        log.info("Mock Http Service is started");

        this.pipelineConfig = initPipelineConfig("pipeline-config.yaml");

        // create topics
//        broker.createTopic(pipelineConfig.getMetersTopic());
//        broker.createTopic(pipelineConfig.getSinkTopic());

        // start events generators
        new TempRecordsGenerator(pipelineConfig).start();
        new PressRecordsGenerator(pipelineConfig).start();

        this.pipeline = createPipeline();
    }

    @AfterEach
    public void afterTest() throws Exception {
        this.enrichmentService.stop();
        log.info("Mock Http Service is stopped");

        this.pipeline.close();
        log.info("Flink pipeline is stopped");
    }


    private PipelineConfig initPipelineConfig(String pipelineConfigFile) throws IOException {
        PipelineConfig pipelineConfig = YAML_MAPPER.readValue(getClass().getResourceAsStream("/" + pipelineConfigFile), PipelineConfig.class);
        pipelineConfig.setKafkaParams(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, pipelineConfig.getKafkaBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer",
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000"
        ));

        pipelineConfig.setEnrichmentEndpoint("http://127.0.0.1:80/enrichment");

        return pipelineConfig;
    }

    protected StreamExecutionEnvironment prepareFlinkEnv() throws IOException {
        Map<String, String> config = pipelineConfig.getFlinkConfig().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        var env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration.fromMap(config));

        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        env.setStateBackend(rocksDBStateBackend);

        return env;
    }
}
