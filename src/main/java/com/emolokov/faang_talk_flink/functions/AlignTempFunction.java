package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.MeterRecord;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.io.Serializable;

public class AlignTempFunction extends RichMapFunction<MeterRecord, MeterRecord> implements Serializable {
    @Override
    public MeterRecord map(MeterRecord record) throws Exception {
        switch (record.getTempUnit()){
            case "K": record.setTempValue(record.getTempValue() - 273); break;
            case "F": record.setTempValue((record.getTempValue() - 32) * 5.0/9.0); break;
            case "C": record.setTempValue(record.getTempValue()); break;
        }

        record.setTempUnit("C");
        return record;
    }
}
