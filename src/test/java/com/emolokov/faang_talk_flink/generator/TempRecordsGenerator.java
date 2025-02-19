package com.emolokov.faang_talk_flink.generator;

import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.MeterTemplate;
import com.emolokov.faang_talk_flink.model.records.TempRecord;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TempRecordsGenerator extends EventsGenerator<TempRecord> {

    // generate meters templates that are used for meters records
    public static final List<MeterTemplate> TEMP_METERS_TEMPLATES = IntStream.range(0, 3)
            .mapToObj(i -> {
                var tempUnit = List.of("K", "C", "F").get(RANDOM.nextInt(3));
                var locationIdx = LOCATIONS.get(RANDOM.nextInt(LOCATIONS.size()));
                return new MeterTemplate(
                        String.format("temp-%03d", locationIdx),
                        String.format("loc-%03d", locationIdx),
                        tempUnit);
            })
            .collect(Collectors.toList());

    private static Supplier<List<TempRecord>> supplier() {
        return () -> {
            MeterTemplate template = TEMP_METERS_TEMPLATES.get(RANDOM.nextInt(TEMP_METERS_TEMPLATES.size()));

            double tempValue;
            switch (template.getUnit()){
                case "C": tempValue = 0   + 20.0 + 10.0 * (RANDOM.nextDouble() - 0.5); break;
                case "K": tempValue = 273 + 20.0 + 10.0 * (RANDOM.nextDouble() - 0.5); break;
                case "F": tempValue = 32  + 36 + 18.0 * (RANDOM.nextDouble() - 0.5); break;
                default: throw new RuntimeException("Unsupported tempUnit: " + template.getUnit());
            }

            var timestamp = System.currentTimeMillis();
            return List.of(
                    new TempRecord(
                            timestamp,
                            template.getMeterId(),
                            template.getLocationId(),
                            template.getUnit(),
                            tempValue)
            );
        };
    }

    public TempRecordsGenerator(PipelineConfig pipelineConfig) {
        super(1,
                pipelineConfig.getKafkaBootstrapServers(),
                pipelineConfig.getTempMetersTopic(),
                supplier());
    }
}
