package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class MeterTemplate implements Serializable {

    public MeterTemplate(String meterId, String tempUnit) {
        this.meterId = meterId;
        this.tempUnit = tempUnit;
    }

    private String meterId;
    private String tempUnit;
}