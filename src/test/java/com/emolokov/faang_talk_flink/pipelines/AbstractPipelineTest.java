package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.FlinkPipeline;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.kafka.MockKafkaBroker;
import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractPipelineTest {
    protected static final Random RANDOM = new Random();
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    protected static final ObjectMapper YAML_MAPPER = new YAMLMapper();


    protected MockKafkaBroker broker;
    protected PipelineConfig pipelineConfig;
    protected FlinkPipeline pipeline;

    protected abstract String pipelineConfigFile();

    protected abstract FlinkPipeline createPipeline() throws IOException;

    @BeforeEach
    public void beforeTest() throws IOException, InterruptedException {
        this.broker = MockKafkaBroker.startNewKafkaBroker("kafka");
        log.info("Mock Kafka Broker is started: {}", broker.getBootstrapServers());

        this.pipelineConfig = initPipelineConfig(pipelineConfigFile());
        updatePipelineConfigByBroker(this.pipelineConfig, broker);

        // create topics
        broker.createTopic(pipelineConfig.getSourceTopic());
        broker.createTopic(pipelineConfig.getSinkTopic());

        this.pipeline = createPipeline();
    }

    @AfterEach
    public void afterTest() throws Exception {
        broker.stop();
        log.info("Mock Kafka Broker is stopped: {}", broker.getBootstrapServers());

        this.pipeline.close();
        log.info("Flink pipeline is stopped");
    }


    private PipelineConfig initPipelineConfig(String pipelineConfigFile) throws IOException {
        PipelineConfig pipelineConfig = YAML_MAPPER.readValue(getClass().getResourceAsStream("/" + pipelineConfigFile), PipelineConfig.class);
        pipelineConfig.setKafkaParams(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer",
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000"
        ));

        pipelineConfig.setSourceTopic("source-topic");
        pipelineConfig.setSinkTopic("sink-topic");

        return pipelineConfig;
    }

    private void updatePipelineConfigByBroker(PipelineConfig pipelineConfig, MockKafkaBroker broker) throws InterruptedException {
        Map<String, String> kafkaParams = new HashMap<>(pipelineConfig.getKafkaParams());
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        pipelineConfig.setKafkaParams(kafkaParams);
    }

    protected StreamExecutionEnvironment prepareFlinkEnv() throws IOException {
        Map<String, String> config = pipelineConfig.getFlinkConfig().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        var env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration.fromMap(config));

        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        env.setStateBackend(rocksDBStateBackend);

        return env;
    }

    protected abstract double messagesPerSec();

    protected abstract Supplier<MeterRecord> meterRecordSupplier();

    protected void produceTestEvents() throws Exception {
        Producer<Bytes, Bytes> producer = newProducer(broker);

        MutableInt eventId = new MutableInt(1);
        RateLimiter produceLimiter = RateLimiter.create(messagesPerSec());
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true){
                produceLimiter.acquire();

                MeterRecord meterRecord = meterRecordSupplier().get();

                try {
                    produceToKafka(pipelineConfig,
                            producer,
                            meterRecord.getMeterId(),
                            JSON_MAPPER.valueToTree(meterRecord),
                            Map.of()
                    );
                } catch (Throwable t) {
                    log.error("Failed to produce test event", t);
                    throw new RuntimeException(t);
                }

                eventId.increment();
            }
        });
    }

    private void produceToKafka(PipelineConfig pipelineConfig,
                                Producer<Bytes, Bytes> producer,
                                String meterId,
                                ObjectNode value,
                                Map<String, String> headers) throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
        ProducerRecord<Bytes, Bytes> producerRecord = new ProducerRecord<>(pipelineConfig.getSourceTopic(),
                Bytes.wrap(meterId.getBytes()),
                Bytes.wrap(JSON_MAPPER.writeValueAsBytes(value)));
        if(headers != null){
            headers.forEach((headerName, headerValue) -> producerRecord.headers().add(headerName, headerValue.getBytes()));
        }

        Future<RecordMetadata> future = producer.send(producerRecord);
        RecordMetadata recordMetadata = future.get(1L, TimeUnit.SECONDS);
        log.info("Produced to {}/{}: {}",
                recordMetadata.topic(),
                recordMetadata.offset(),
                value.toString()
        );
    }

    private Producer<Bytes, Bytes> newProducer(MockKafkaBroker broker) {
        Properties configs = new Properties();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        return new KafkaProducer<>(configs);
    }
}
