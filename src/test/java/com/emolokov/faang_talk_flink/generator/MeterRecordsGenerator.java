package com.emolokov.faang_talk_flink.generator;

import com.emolokov.faang_talk_flink.model.MeterRecord;
import com.emolokov.faang_talk_flink.model.MeterTemplate;
import com.emolokov.faang_talk_flink.model.PipelineConfig;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emolokov.faang_talk_flink.model.TempUnit.*;

public class MeterRecordsGenerator extends EventsGenerator<MeterRecord> {

    // generate meters templates that are used for meters records
    public static final List<MeterTemplate> METER_TEMPLATES = IntStream.range(0, 3)
            .mapToObj(i -> {
                var tempUnit = List.of(K, C, F).get(RANDOM.nextInt(3));
                return new MeterTemplate(
                                UUID.randomUUID().toString(),
                                tempUnit);
            })
            .collect(Collectors.toList());

    private static Supplier<List<MeterRecord>> supplier() {
        return () -> {
            MeterTemplate template = METER_TEMPLATES.get(RANDOM.nextInt(METER_TEMPLATES.size()));

            double tempValue;
            switch (template.getTempUnit()){
                case C: tempValue = 0   + 20.0 + 10.0 * (RANDOM.nextDouble() - 0.5); break;
                case K: tempValue = 273 + 20.0 + 10.0 * (RANDOM.nextDouble() - 0.5); break;
                case F: tempValue = 32  + 36 + 18.0 * (RANDOM.nextDouble() - 0.5); break;
                default: throw new RuntimeException("Unsupported tempUnit: " + template.getTempUnit());
            }

            return List.of(
                    new MeterRecord(
                            template.getMeterId(),
                            System.currentTimeMillis(),
                            template.getTempUnit(),
                            tempValue)
            );
        };
    }

    public MeterRecordsGenerator(PipelineConfig pipelineConfig) {
        super(pipelineConfig.getMetersRecordsGenRatePerSec(),
                pipelineConfig.getKafkaBrokerBootstrapServers(),
                pipelineConfig.getMetersTopic(),
                MeterRecord.class,
                supplier());
    }
}
