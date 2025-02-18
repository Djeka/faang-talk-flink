package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.MeterRecord;
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

public class DeduplicateFunction<R extends MeterRecord> extends RichFlatMapFunction<R, R> {

    private final PipelineConfig pipelineConfig;
    private final Duration windowDuration;

    private ValueState<Boolean> recordsState;

    public DeduplicateFunction(PipelineConfig pipelineConfig, Duration windowDuration) {
        this.pipelineConfig = pipelineConfig;
        this.windowDuration = windowDuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("deduplicate-records-state", Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(Time.seconds(windowDuration.getSeconds()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build());

        this.recordsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(R record, Collector<R> out) throws Exception {
        boolean seenMeterRecord = Optional.ofNullable(this.recordsState.value()).orElse(false);
        this.recordsState.update(true);

        if(seenMeterRecord) {
            // mark as duplicate
            record.setDuplicate(true);
        }

        out.collect(record);
    }
}
