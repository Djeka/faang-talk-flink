package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@ToString
public class MeterRecord extends AbstractRecord implements Serializable {

    public MeterRecord(String meterId, long timestamp, String tempUnit, double tempValue) {
        super(timestamp);
        this.meterId = meterId;
        this.tempUnit = tempUnit;
        this.tempValue = tempValue;
    }

    private String meterId;
    private String tempUnit;
    private double tempValue;

    private Boolean duplicate;
    private String meterName;
    private Double price;

    @Override
    public String getId() {
        return meterId;
    }
}
