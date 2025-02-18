package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.EnrichmentFunction;
import com.emolokov.faang_talk_flink.model.records.TempRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class EnrichmentPipeline extends FlinkPipeline {

    public EnrichmentPipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<TempRecord> metersStream = createSource(pipelineConfig.getTempMetersTopic(), TempRecord.class, 1);

        // save to state
        var stream = enrich(metersStream)
                .name("enriched-stream");

//        stream.filter(v -> false).print();
//        stream.print();
        stream.sinkTo(sink()).name("sink");
    }

    private SingleOutputStreamOperator<TempRecord> enrich(DataStream<TempRecord> input){
        return AsyncDataStream.orderedWait(
                input,
                new EnrichmentFunction(pipelineConfig),
                1L, SECONDS,
                10
        );
    }
}
