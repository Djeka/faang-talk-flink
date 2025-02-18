package com.emolokov.faang_talk_flink.model.records;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@ToString
public abstract class MeterRecord extends Record {
    private long eventTimestamp;
    private String meterId;
    private String locationId;

    private Boolean duplicate;
    private String meterName;

    public MeterRecord(long timestamp, String meterId, String locationId) {
        this.eventTimestamp = timestamp;
        this.meterId = meterId;
        this.locationId = locationId;
    }
}
