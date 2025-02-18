package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.AlignTempFunction;
import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class AlignTempPipeline extends FlinkPipeline {

    public AlignTempPipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<MeterRecord> metersStream = createSource(pipelineConfig.getMetersTopic(), MeterRecord.class, 1);

        // save to state
        var stream = metersStream
                // align temp
                .map(new AlignTempFunction())
                .name("aligned-stream");

//        stream.filter(v -> false).print();
//        stream.print();
        stream.sinkTo(sink()).name("sink");
    }
}
