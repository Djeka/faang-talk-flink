package com.emolokov.faang_talk_flink;

import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.pipelines.impl.AlignTempPipeline;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var pipelineConfig = PipelineConfig.load("main-pipeline-config.yaml");
        AlignTempPipeline pipeline = new AlignTempPipeline(pipelineConfig, env);
        pipeline.run();
    }
}
