package com.emolokov.faang_talk_flink.pipelines.impl;

import com.emolokov.faang_talk_flink.functions.SqlJoin;
import com.emolokov.faang_talk_flink.model.records.JoinedRecord;
import com.emolokov.faang_talk_flink.model.records.TempRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.PressRecord;
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
        DataStream<TempRecord> tempStream = createSource(pipelineConfig.getTempMetersTopic(), TempRecord.class, 1);
        DataStream<PressRecord> pressStream = createSource(pipelineConfig.getPressMetersTopic(), PressRecord.class, 1);

        String sqlQuery = ""
                + "SELECT "
                + "    t.locationId, " + "\n"
                + "    t.eventTimestamp AS tempTimestamp, " + "\n"
                + "    t.meterId AS tempMeterId, " + "\n"
                + "    t.tempUnit AS tempUnit, " + "\n"
                + "    t.tempValue AS tempValue, " + "\n"
                + "    p.eventTimestamp AS pressTimestamp, " + "\n"
                + "    p.meterId AS pressMeterId, " + "\n"
                + "    p.pressUnit AS pressUnit, " + "\n"
                + "    p.pressValue AS pressValue " + "\n"
                + "FROM TEMP_RECORDS AS t " + "\n"
                + "JOIN PRESS_RECORDS AS p " + "\n"
                + "ON t.locationId = p.locationId " + "\n"
                + "  AND p.ts BETWEEN t.ts - INTERVAL '60' SECOND "  + "\n" +
                                 "AND t.ts + INTERVAL '5' SECOND";

        // apply sql
        DataStream<JoinedRecord> stream = new SqlJoin(env).sqlJoin(tempStream, pressStream, sqlQuery);

//        stream.filter(v -> false).print();
        stream.print();
//        stream.sinkTo(sink()).name("sink");
    }
}
