package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class MeterRecord implements Serializable {

    public enum TempUnit{
        C, // Celsius
        F, // Fahrenheit
        K // Kelvin
    }

    public MeterRecord(String meterId, long timestamp, TempUnit tempUnit, double tempValue) {
        this.meterId = meterId;
        this.timestamp = timestamp;
        this.tempUnit = tempUnit;
        this.tempValue = tempValue;
    }

    private String meterId;
    private long timestamp;
    private TempUnit tempUnit;
    private double tempValue;

    private double avgTemp;
}
