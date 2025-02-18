package com.emolokov.faang_talk_flink.model.records;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@ToString
public class TempRecord extends MeterRecord {

    private String tempUnit;
    private double tempValue;

    public TempRecord(long timestamp, String meterId, String locationId, String tempUnit, double tempValue) {
        super(timestamp, meterId, locationId);
        this.tempUnit = tempUnit;
        this.tempValue = tempValue;
    }

    @Override
    public String toString() {
        return "TempRecord{" +
                "tempUnit=" + tempUnit +
                ", tempValue=" + tempValue +
                ", meterId=" + getMeterId() +
                ", locationId=" + getLocationId() +
                ", eventTimestamp=" + getEventTimestamp() +
                ", duplicate=" + getDuplicate() +
                ", meterName=" + getMeterName() +
                '}';
    }
}
