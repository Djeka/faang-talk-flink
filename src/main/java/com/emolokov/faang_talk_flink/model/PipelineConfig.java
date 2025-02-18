package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
public class PipelineConfig implements Serializable {
    @JsonProperty("meters-topic")
    private String metersTopic;

    @JsonProperty("sink-topic")
    private String sinkTopic;

    @JsonProperty("price-topic")
    private String priceTopic;

    @JsonProperty("kafka-params")
    private Map<String, String> kafkaParams;

    @JsonProperty("kafka-broker-bootstrap-servers")
    private String kafkaBrokerBootstrapServers;

    @JsonProperty("meters-records-gen-rate-per-sec")
    private Double metersRecordsGenRatePerSec = 0.0;

    @JsonProperty("price-records-gen-rate-per-sec")
    private Double priceRecordsGenRatePerSec = 0.0;

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
