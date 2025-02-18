package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.SqlJoin;
import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.PriceRecord;
import com.emolokov.faang_talk_flink.pipelines.FlinkPipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class SqlPipeline extends FlinkPipeline {

    public SqlPipeline(PipelineConfig pipelineConfig, StreamExecutionEnvironment env) {
        super(pipelineConfig, env);
    }

    @Override
    protected void buildFlinkPipeline(){
        // get source data from the topic
        DataStream<MeterRecord> metersStream = createSource(pipelineConfig.getMetersTopic(), MeterRecord.class, 1);
        DataStream<PriceRecord> priceStream = createSource(pipelineConfig.getPriceTopic(), PriceRecord.class, 1);

        String sqlQuery = ""
                + "SELECT "
                + "    m.*, "
                + "    p.price AS price "
                + "FROM METERS AS m "
                + "JOIN PRICES AS p "
                + "ON m.meterId = p.meterId "
                + "  AND p.ts BETWEEN m.ts - INTERVAL '60' SECOND " +
                                 "AND m.ts + INTERVAL '5' SECOND";

        // apply sql
        DataStream<MeterRecord> stream = new SqlJoin(env).sqlJoin(metersStream, priceStream, sqlQuery);

//        stream.filter(v -> false).print();
        stream.print();
//        stream.sinkTo(sink()).name("sink");
    }
}
