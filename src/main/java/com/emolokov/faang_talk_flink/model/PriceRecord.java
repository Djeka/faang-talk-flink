package com.emolokov.faang_talk_flink.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
@NoArgsConstructor
public class PriceRecord extends AbstractRecord implements Serializable {
    private String meterId;
    private Double price;

    public PriceRecord(String meterId, Double price, Long timestamp) {
        super(timestamp);
        this.meterId = meterId;
        this.price = price;
    }

    @Override
    public String getId() {
        return meterId;
    }
}
