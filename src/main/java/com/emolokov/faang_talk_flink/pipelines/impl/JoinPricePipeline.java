package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.JoinPriceFunction;
import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.PriceRecord;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

@Slf4j
public class JoinPricePipeline extends FlinkPipeline {

    public JoinPricePipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<MeterRecord> metersStream = createSource(pipelineConfig.getMetersTopic(), MeterRecord.class, 1);
        DataStream<PriceRecord> priceStream = createSource(pipelineConfig.getPriceTopic(), PriceRecord.class, 1);

        KeyedStream<MeterRecord, String> keyedMeters = metersStream
                .keyBy(MeterRecord::getMeterId);

        KeyedStream<PriceRecord, String> keyedPrices = priceStream
                .keyBy(PriceRecord::getMeterId);


        // save to state
        DataStream<MeterRecord> stream = keyedMeters
                        .connect(keyedPrices)
                .process(new JoinPriceFunction(pipelineConfig, Duration.ofSeconds(60), Duration.ofSeconds(10)))
                .name("joined-stream");

//        stream.filter(v -> false).print();
//        stream.print();
        stream.sinkTo(sink()).name("sink");
    }
}
