package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.records.JoinedRecord;
import com.emolokov.faang_talk_flink.model.records.TempRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.PressRecord;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

public class JoinMetersFunction extends KeyedCoProcessFunction<Long, TempRecord, PressRecord, JoinedRecord> {

    private final PipelineConfig pipelineConfig;
    private final Duration joinWindowDuration;

    private ListState<TempRecord> tempRecordsBufferState;
    private ListState<PressRecord> pressRecordsBufferState;

    public JoinMetersFunction(PipelineConfig pipelineConfig, Duration joinWindowDuration) {
        this.pipelineConfig = pipelineConfig;
        this.joinWindowDuration = joinWindowDuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<TempRecord> tempDescriptor = new ListStateDescriptor<>("temp-records-buffer-state", TempRecord.class);
        this.tempRecordsBufferState = getRuntimeContext().getListState(tempDescriptor);

        ListStateDescriptor<PressRecord> pressDescriptor = new ListStateDescriptor<>("press-records-buffer-state", PressRecord.class);
        this.pressRecordsBufferState = getRuntimeContext().getListState(pressDescriptor);
    }


    @Override
    public void processElement1(TempRecord tempRecord, KeyedCoProcessFunction<Long, TempRecord, PressRecord, JoinedRecord>.Context ctx, Collector<JoinedRecord> out) throws Exception {
        tempRecordsBufferState.add(tempRecord);

        // Enrich any buffered MeterRecords
        Iterable<PressRecord> bufferedRecords = pressRecordsBufferState.get();
        if (bufferedRecords != null) {
            bufferedRecords.forEach(pressRecord -> out.collect(new JoinedRecord(tempRecord, pressRecord)));
            pressRecordsBufferState.clear();
        } else {
            // Register a processing-time timer for (now + join window duration)
            // This ensures we don't wait forever for a record.
            long currentTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentTime + joinWindowDuration.toMillis());
        }
    }

    @Override
    public void processElement2(PressRecord pressRecord, KeyedCoProcessFunction<Long, TempRecord, PressRecord, JoinedRecord>.Context ctx, Collector<JoinedRecord> out) throws Exception {
        pressRecordsBufferState.add(pressRecord);

        // Enrich any buffered MeterRecords
        Iterable<TempRecord> bufferedRecords = tempRecordsBufferState.get();
        if (bufferedRecords != null) {
            bufferedRecords.forEach(tempRecord -> out.collect(new JoinedRecord(tempRecord, pressRecord)));
            tempRecordsBufferState.clear();
        } else {
            // Register a processing-time timer for (now + join window duration)
            // This ensures we don't wait forever for a record.
            long currentTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentTime + joinWindowDuration.toMillis());
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<JoinedRecord> out
    ) throws Exception {
        // If we reach here, it means join window has passed
        // since we buffered at least one MeterRecord
        // and no other record arrived in that time (otherwise we'd have cleared the buffer).
        Optional.ofNullable(tempRecordsBufferState.get())
                .ifPresent(buffer -> {
                    buffer.forEach(tempRecord -> out.collect(new JoinedRecord(tempRecord, null)));
                });
        tempRecordsBufferState.clear();

        Optional.ofNullable(pressRecordsBufferState.get())
                .ifPresent(buffer -> {
                    buffer.forEach(pressRecord -> out.collect(new JoinedRecord(null, pressRecord)));
                });
        pressRecordsBufferState.clear();
    }
}
