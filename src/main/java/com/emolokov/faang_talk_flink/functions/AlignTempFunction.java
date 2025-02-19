package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.records.TempRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.io.Serializable;

@Slf4j
public class AlignTempFunction extends RichMapFunction<TempRecord, TempRecord> implements Serializable {
    @Override
    public TempRecord map(TempRecord record) throws Exception {
        switch (record.getTempUnit()){
            case "C": break;
            case "K": record.setTempValue(record.getTempValue() - 273); break;
            case "F": record.setTempValue((record.getTempValue() - 32) * 5.0/9.0); break;
            default: throw new UnsupportedOperationException("Unsupported temp unit " + record.getTempUnit());
        }

        record.setTempUnit("C");
        log.info("Aligned {} temperature", record.getTempValue());
        return record;
    }
}
