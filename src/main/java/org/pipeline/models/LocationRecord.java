package org.pipeline.models;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LocationRecord {
    private Instant datetime; // time
    private float value; // field
    private int sensorsId; // tags
    private int locationsId; // tags
    private double latitude; // tags
    private double longitude; // tags
}