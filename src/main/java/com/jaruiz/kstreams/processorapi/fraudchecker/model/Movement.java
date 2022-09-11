package com.jaruiz.kstreams.processorapi.fraudchecker.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Movement {

    String id;
    String card;
    float amount;
    long origin;
    String site;
    String device;
    String createdAt;

}
