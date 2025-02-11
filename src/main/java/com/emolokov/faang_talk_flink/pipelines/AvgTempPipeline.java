package com.emolokov.faang_talk_flink.pipelines;

import com.emolokov.faang_talk_flink.FlinkPipeline;
import com.emolokov.faang_talk_flink.functions.AvgTempFunction;
import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

@Slf4j
public class AvgTempPipeline extends FlinkPipeline {

    public AvgTempPipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    public void run(){
        buildFlinkPipeline();

        try {
            env.setParallelism(pipelineConfig.getParallelism());
            env.execute();
        } catch (Exception e) {
            log.error("Failed to execute Flink job");
            throw new RuntimeException(e);
        }
    }

    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<MeterRecord> sourceStream = createSource();

        // save to state
        var stream = sourceStream
                .keyBy(MeterRecord::getMeterId) // key by meter_id
                .flatMap(new AvgTempFunction(pipelineConfig, Duration.ofSeconds(60))) // use a flat map function with a TTL state
                .name("stated-stream");

//        statefulStream.filter(v -> false).print();
        stream.print();
    }

}
