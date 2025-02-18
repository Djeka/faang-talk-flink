package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.PriceRecord;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class JoinPriceFunction extends KeyedCoProcessFunction<String, MeterRecord, PriceRecord, MeterRecord> {

    private final PipelineConfig pipelineConfig;
    private final Duration priceLifeTime;
    private final Duration joinWindowDuration;

    // Store the latest PriceRecord for this meterId
    private ValueState<Double> priceState;
    private ListState<MeterRecord> meterBufferState;

    public JoinPriceFunction(PipelineConfig pipelineConfig, Duration priceLifeTime, Duration joinWindowDuration) {
        this.pipelineConfig = pipelineConfig;
        this.priceLifeTime = priceLifeTime;
        this.joinWindowDuration = joinWindowDuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> priceDescriptor = new ValueStateDescriptor<>("price-state", Types.DOUBLE);
        priceDescriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(Time.seconds(priceLifeTime.getSeconds()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build());
        this.priceState = getRuntimeContext().getState(priceDescriptor);

        ListStateDescriptor<MeterRecord> meterDescriptor = new ListStateDescriptor<>("meters-state", MeterRecord.class);
        this.meterBufferState = getRuntimeContext().getListState(meterDescriptor);
    }


    @Override
    public void processElement1(MeterRecord meterRecord, KeyedCoProcessFunction<String, MeterRecord, PriceRecord, MeterRecord>.Context ctx, Collector<MeterRecord> out) throws Exception {
        Double currentPrice = priceState.value();

        if (currentPrice != null) {
            meterRecord.setPrice(currentPrice);
        } else {
            meterBufferState.add(meterRecord);

            // Register a processing-time timer for (now + join window duration)
            // This ensures we don't wait forever for a PriceRecord.
            long currentTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentTime + joinWindowDuration.toMillis());
        }

        out.collect(meterRecord);
    }

    @Override
    public void processElement2(PriceRecord priceRecord, KeyedCoProcessFunction<String, MeterRecord, PriceRecord, MeterRecord>.Context ctx, Collector<MeterRecord> out) throws Exception {
        priceState.update(priceRecord.getPrice());

        // Enrich any buffered MeterRecords
        Iterable<MeterRecord> bufferedMeters = meterBufferState.get();
        if (bufferedMeters != null) {
            try {
                for (MeterRecord meterRecord : bufferedMeters) {
                    meterRecord.setPrice(priceRecord.getPrice());
                    out.collect(meterRecord);
                }
            } finally {
                // Clear them from the buffer after processing
                meterBufferState.clear();
            }
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<MeterRecord> out
    ) throws Exception {
        // If we reach here, it means join window has passed
        // since we buffered at least one MeterRecord
        // and no PriceRecord arrived in that time (otherwise we'd have cleared the buffer).
        Iterable<MeterRecord> bufferedMeters = meterBufferState.get();
        if (bufferedMeters != null) {
            try {
                // Business decision: do you want to emit partial data or discard?
                // Option A: Discard them if you cannot proceed without price
                // Option B: Emit partial results, e.g., EnrichedMeterRecord with price=0 or null

                for (MeterRecord meterRecord : bufferedMeters) {
                    out.collect(meterRecord);
                }
            } finally {
                // Clear the buffer
                meterBufferState.clear();
            }
        }
    }
}
