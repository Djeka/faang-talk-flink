package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
public class PipelineConfig implements Serializable {
    @JsonProperty("temp-meters-topic")
    private String tempMetersTopic;

    @JsonProperty("press-meters-topic")
    private String pressMetersTopic;

    @JsonProperty("sink-topic")
    private String sinkTopic;

    @JsonProperty("kafka-params")
    private Map<String, String> kafkaParams;

    @JsonProperty("kafka-bootstrap-servers")
    private String kafkaBootstrapServers;

    @JsonProperty("disable-operators-chain")
    private boolean disableOperatorsChain = false;

    @JsonProperty("parallelism")
    private int parallelism = 1;

    @JsonProperty("enrichment-endpoint")
    private String enrichmentEndpoint;

    @JsonProperty("deduplicate-window-sec")
    private Integer deduplicateWindowSec = 60;

    @JsonProperty("flink-config")
    private Map<String, Object> flinkConfig;
}
