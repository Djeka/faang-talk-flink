package com.emolokov.faang_talk_flink.model.records;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class PressRecord extends MeterRecord {
    private String pressUnit;
    private double pressValue;

    public PressRecord(long timestamp, String meterId, String locationId, String pressUnit, double pressValue) {
        super(timestamp, meterId, locationId);
        this.pressUnit = pressUnit;
        this.pressValue = pressValue;
    }

    @Override
    public String toString() {
        return "TempRecord{" +
                "tempUnit=" + pressUnit +
                ", tempValue=" + pressValue +
                ", meterId=" + getMeterId() +
                ", locationId=" + getLocationId() +
                ", eventTimestamp=" + getEventTimestamp() +
                ", duplicate=" + getDuplicate() +
                ", meterName=" + getMeterName() +
                '}';
    }
}
