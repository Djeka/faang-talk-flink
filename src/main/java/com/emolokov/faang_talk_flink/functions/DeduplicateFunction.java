package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

public class DeduplicateFunction extends RichFlatMapFunction<MeterRecord, MeterRecord> {

    private final PipelineConfig pipelineConfig;
    private final Duration windowDuration;

    private ValueState<Boolean> metersRecords;

    public DeduplicateFunction(PipelineConfig pipelineConfig, Duration windowDuration) {
        this.pipelineConfig = pipelineConfig;
        this.windowDuration = windowDuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("deduplicate-meters-records", Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(Time.seconds(windowDuration.getSeconds()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build());

        this.metersRecords = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(MeterRecord record, Collector<MeterRecord> out) throws Exception {
        boolean seenMeterRecord = Optional.ofNullable(this.metersRecords.value()).orElse(false);
        this.metersRecords.update(true);

        if(seenMeterRecord) {
            // mark as duplicate
            record.setDuplicate(true);
        }

        out.collect(record);
    }
}
