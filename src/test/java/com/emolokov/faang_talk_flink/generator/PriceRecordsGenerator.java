package com.emolokov.faang_talk_flink.generator;

import com.emolokov.faang_talk_flink.model.MeterTemplate;
import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.PriceRecord;

import java.util.List;
import java.util.function.Supplier;

import static com.emolokov.faang_talk_flink.generator.MeterRecordsGenerator.METER_TEMPLATES;

public class PriceRecordsGenerator extends EventsGenerator<PriceRecord> {

    private static Supplier<List<PriceRecord>> supplier() {
        return () -> {
            MeterTemplate template = METER_TEMPLATES.get(RANDOM.nextInt(METER_TEMPLATES.size()));

            return List.of(
                    new PriceRecord(
                            template.getMeterId(),
                            10.0 * RANDOM.nextDouble(),
                            System.currentTimeMillis())
            );
        };
    }

    public PriceRecordsGenerator(PipelineConfig pipelineConfig) {
        super(pipelineConfig.getPriceRecordsGenRatePerSec(),
                pipelineConfig.getKafkaBrokerBootstrapServers(),
                pipelineConfig.getPriceTopic(),
                PriceRecord.class,
                supplier());
    }
}
