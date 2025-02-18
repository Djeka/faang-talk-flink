package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.DeduplicateFunction;
import com.emolokov.faang_talk_flink.model.records.TempRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.functions.AlignTempFunction;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

@Slf4j
public class DeduplicatePipeline extends FlinkPipeline {

    public DeduplicatePipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<TempRecord> metersStream = createSource(pipelineConfig.getTempMetersTopic(), TempRecord.class, 1);

        // save to state
        var stream = metersStream
                // align temp
                .map(new AlignTempFunction())
                .keyBy(TempRecord::getMeterId) // key by meter_id
                .flatMap(new DeduplicateFunction<TempRecord>(pipelineConfig, Duration.ofSeconds(60))) // use a flat map function with a TTL state
                .name("stated-stream");

//        statefulStream.filter(v -> false).print();
        stream.print();
//        stream.sinkTo(sink()).name("sink");
    }

}
