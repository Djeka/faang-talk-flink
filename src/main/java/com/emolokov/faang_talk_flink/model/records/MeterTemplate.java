package com.emolokov.faang_talk_flink.model.records;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class MeterTemplate implements Serializable {

    private String meterId;
    private String locationId;
    private String unit;

    public MeterTemplate(String meterId, String locationId, String unit) {
        this.meterId = meterId;
        this.locationId = locationId;
        this.unit = unit;
    }
}