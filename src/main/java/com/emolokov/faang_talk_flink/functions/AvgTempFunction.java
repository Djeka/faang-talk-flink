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
import java.util.ArrayList;
import java.util.List;

public class AvgTempFunction extends RichFlatMapFunction<MeterRecord, MeterRecord> {

    private final PipelineConfig pipelineConfig;
    private final Duration windowDuration;

    private ValueState<List<Double>> tempValues;

    public AvgTempFunction(PipelineConfig pipelineConfig, Duration windowDuration) {
        this.pipelineConfig = pipelineConfig;
        this.windowDuration = windowDuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<List<Double>> descriptor = new ValueStateDescriptor<>("temp_values", Types.LIST(Types.DOUBLE));
        descriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(Time.seconds(windowDuration.getSeconds()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build());

        this.tempValues = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(MeterRecord record, Collector<MeterRecord> out) throws Exception {
        List<Double> tempValues = this.tempValues.value();
        if(tempValues == null) {
            tempValues = new ArrayList<>();
        }

        tempValues.add(record.getTempValue());
        record.setAvgTemp(tempValues.stream().mapToDouble(v -> v).average().getAsDouble());

        out.collect(record);
    }
}
