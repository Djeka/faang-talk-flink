package com.emolokov.faang_talk_flink.generator;

import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.MeterTemplate;
import com.emolokov.faang_talk_flink.model.records.PressRecord;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PressRecordsGenerator extends EventsGenerator<PressRecord> {

    // generate meters templates that are used for meters records
    public static final List<MeterTemplate> PRESS_METERS_TEMPLATES = IntStream.range(0, 3)
            .mapToObj(i -> {
                var pressUnit = List.of("Pa", "Atm", "Bar", "psi").get(RANDOM.nextInt(4));
                var locationIdx = LOCATIONS.get(RANDOM.nextInt(LOCATIONS.size()));
                return new MeterTemplate(
                        String.format("press-%03d", locationIdx),
                        String.format("loc-%03d", locationIdx),
                        pressUnit);
            })
            .collect(Collectors.toList());

    private static Supplier<List<PressRecord>> supplier() {
        return () -> {
            MeterTemplate template = PRESS_METERS_TEMPLATES.get(RANDOM.nextInt(PRESS_METERS_TEMPLATES.size()));

            double pressValue;
            switch (template.getUnit()){
                case "Pa": pressValue = 0   + 20.0 + 10.0 * (RANDOM.nextDouble() - 0.5); break;
                case "Atm": pressValue = 273 + 20.0 + 10.0 * (RANDOM.nextDouble() - 0.5); break;
                case "Bar": pressValue = 32  + 36 + 18.0 * (RANDOM.nextDouble() - 0.5); break;
                case "psi": pressValue = 32  + 36 + 18.0 * (RANDOM.nextDouble() - 0.5); break;
                default: throw new RuntimeException("Unsupported unit: " + template.getUnit());
            }

            return List.of(
                    new PressRecord(
                            System.currentTimeMillis(),
                            template.getMeterId(),
                            template.getLocationId(),
                            template.getUnit(),
                            pressValue)
            );
        };
    }

    public PressRecordsGenerator(PipelineConfig pipelineConfig) {
        super(1,
                pipelineConfig.getKafkaBootstrapServers(),
                pipelineConfig.getPressMetersTopic(),
                supplier());
    }
}
