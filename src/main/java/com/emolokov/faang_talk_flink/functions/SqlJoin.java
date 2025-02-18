package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.records.JoinedRecord;
import com.emolokov.faang_talk_flink.model.records.TempRecord;
import com.emolokov.faang_talk_flink.model.records.PressRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SqlJoin {

    private static final ObjectMapper JSON_MAPPER = Optional.of(new ObjectMapper()).stream()
            .peek(m -> m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false))
            .findFirst().get();

    private final StreamTableEnvironment tableEnv;

    public SqlJoin(StreamExecutionEnvironment env) {
        this.tableEnv = StreamTableEnvironment.create(env);
    }

    public DataStream<JoinedRecord> sqlJoin(DataStream<TempRecord> meterStream, DataStream<PressRecord> pressStream, String sqlQuery) {
        // Register the TempRecord stream
        tableEnv.createTemporaryView(
                "TEMP_RECORDS", // the SQL table name
                meterStream,
                Schema.newBuilder()
                        .column("meterId", DataTypes.STRING())
                        .column("locationId", DataTypes.STRING())
                        .column("tempUnit", DataTypes.STRING())
                        .column("tempValue", DataTypes.DOUBLE())
                        .column("duplicate", DataTypes.BOOLEAN())
                        .column("meterName", DataTypes.STRING())
                        // Event-time column with watermark
                        .column("eventTimestamp", DataTypes.BIGINT())
                        .columnByExpression(
                                "ts",
                                "TO_TIMESTAMP_LTZ(eventTimestamp, 3)"
                        )
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build()
        );

        // Register the PressRecord stream
        tableEnv.createTemporaryView(
                "PRESS_RECORDS", // the SQL table name
                pressStream,
                Schema.newBuilder()
                        .column("meterId", DataTypes.STRING())
                        .column("locationId", DataTypes.STRING())
                        .column("pressUnit", DataTypes.STRING())
                        .column("pressValue", DataTypes.DOUBLE())
                        .column("duplicate", DataTypes.BOOLEAN())
                        .column("meterName", DataTypes.STRING())
                        // Event-time column with watermark
                        .column("eventTimestamp", DataTypes.BIGINT())
                        .columnByExpression(
                                "ts",
                                "TO_TIMESTAMP_LTZ(eventTimestamp, 3)"
                        )
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build()
        );

        return tableEnv.toDataStream(tableEnv.sqlQuery(sqlQuery))
                .map(row -> {
                    Map<String, Object> recordMap = new HashMap<>();
                    int fieldIdx = 0;
                    for(var it = row.getFieldNames(true).iterator(); it.hasNext();){
                        var fieldName = it.next();
                        var fieldValue = row.getField(fieldIdx++);
                        recordMap.put(fieldName, String.valueOf(fieldValue));
                    }

                    return JSON_MAPPER.convertValue(recordMap, JoinedRecord.class);
                });

//        Table resultTable = tableEnv.sqlQuery(sqlQuery);
//        return tableEnv.toDataStream(resultTable, JoinedRecord.class);
    }
}
