package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.records.PressRecord;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.io.Serializable;

public class AlignPressFunction extends RichMapFunction<PressRecord, PressRecord> implements Serializable {
    @Override
    public PressRecord map(PressRecord record) throws Exception {
        switch (record.getPressUnit()){
            case "Pa": break;
            case "Atm": record.setPressValue(record.getPressValue() * 101325); break;
            case "Bar": record.setPressValue(record.getPressValue() * 100000); break;
            case "psi": record.setPressValue(record.getPressValue() * 6894.757293168361); break;
            default: throw new UnsupportedOperationException("Unsupported press unit " + record.getPressUnit());
        }

        record.setPressUnit("Pa");
        return record;
    }
}
