package com.emolokov.faang_talk_flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractRecord {
    private long eventTimestamp;

    @JsonIgnore
    public abstract String getId();
}
