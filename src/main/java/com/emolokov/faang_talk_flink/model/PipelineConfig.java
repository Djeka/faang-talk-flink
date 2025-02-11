package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
public class PipelineConfig implements Serializable {
    @JsonProperty("source-topic")
    private String sourceTopic;

    @JsonProperty("sink-topic")
    private String sinkTopic;

    @JsonProperty("kafka-params")
    private Map<String, String> kafkaParams;

    @JsonProperty("parallelism")
    private int parallelism = 1;

    @JsonProperty("flink-config")
    private Map<String, Object> flinkConfig;
}
