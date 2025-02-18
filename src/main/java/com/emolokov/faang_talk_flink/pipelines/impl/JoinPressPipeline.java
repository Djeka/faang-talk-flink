package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.JoinMetersFunction;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.JoinedRecord;
import com.emolokov.faang_talk_flink.model.records.PressRecord;
import com.emolokov.faang_talk_flink.model.records.TempRecord;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

@Slf4j
public class JoinPressPipeline extends FlinkPipeline {

    public JoinPressPipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<TempRecord> tempStream = createSource(pipelineConfig.getTempMetersTopic(), TempRecord.class, 1);
        DataStream<PressRecord> pressStream = createSource(pipelineConfig.getPressMetersTopic(), PressRecord.class, 1);

//        // align units
//        tempStream = tempStream.map(new AlignTempFunction()).name("temp-align");
//        pressStream = pressStream.map(new AlignPressFunction()).name("press-align");

//        // enrich
//        tempStream = enrich(tempStream).name("temp-enriched");
//        pressStream = enrich(pressStream).name("press-enriched");

//        // deduplicate
//        tempStream = tempStream
//                .keyBy(r -> r.getLocationId())
//                .flatMap(new DeduplicateFunction<TempRecord>(pipelineConfig, Duration.ofSeconds(5)))
//                .name("temp-deduplicate");
//        pressStream = pressStream
//                .keyBy(r -> r.getLocationId())
//                .flatMap(new DeduplicateFunction<PressRecord>(pipelineConfig, Duration.ofSeconds(5)))
//                .name("press-deduplicate");

        // key by location
        DataStream<TempRecord> keyedTemp = tempStream.keyBy(r -> r.getLocationId());
        DataStream<PressRecord> keyedPress = pressStream.keyBy(r -> r.getLocationId());

        // join streams
        DataStream<JoinedRecord> stream = keyedTemp.connect(keyedPress)
                .process(new JoinMetersFunction(pipelineConfig, Duration.ofSeconds(10)))
                .name("joined");

//        stream.filter(v -> false).print();
        stream.print();
//        stream.sinkTo(sink()).name("sink");
    }
}
