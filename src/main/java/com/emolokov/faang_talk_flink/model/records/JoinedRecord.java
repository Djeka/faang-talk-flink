package com.emolokov.faang_talk_flink.model.records;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class JoinedRecord extends Record {

    private String locationId;

    private long tempTimestamp;
    private String tempMeterId;
    private String tempUnit;
    private double tempValue;

    private long pressTimestamp;
    private String pressMeterId;
    private String pressUnit;
    private double pressValue;

    public JoinedRecord(TempRecord tempRecord, PressRecord pressRecord) {
        this.locationId = tempRecord != null ? tempRecord.getLocationId() : pressRecord.getLocationId();
        setTempRecord(tempRecord);
        setPressRecord(pressRecord);
    }

    private void setTempRecord(TempRecord tempRecord) {
        if(tempRecord == null) return;

        this.tempTimestamp = tempRecord.getEventTimestamp();
        this.tempMeterId = tempRecord.getMeterId();
        this.tempUnit = tempRecord.getTempUnit();
        this.tempValue = tempRecord.getTempValue();
    }

    private void setPressRecord(PressRecord pressRecord) {
        if(pressRecord == null) return;

        this.pressTimestamp = pressRecord.getEventTimestamp();
        this.pressMeterId = pressRecord.getMeterId();
        this.pressUnit = pressRecord.getPressUnit();
        this.pressValue = pressRecord.getPressValue();
    }

    @Override
    public String toString() {
        return "TempRecord{" +
                "tempUnit=" + tempUnit +
                ", tempValue=" + tempValue +
                ", pressUnit=" + pressUnit +
                ", pressValue=" + pressValue +
                ", meterId=" + pressMeterId +
                ", locationId=" + locationId +
                ", tempTimestamp=" + tempTimestamp +
                ", pressTimestamp=" + pressTimestamp +
                '}';
    }
}
