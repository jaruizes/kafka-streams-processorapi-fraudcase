package com.jaruiz.kstreams.processorapi.fraudchecker.processors;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class OnlineMovementsAggregatorProcessor extends MovementsAggregatorProcessor {

    public OnlineMovementsAggregatorProcessor() {
        super();
        this.stateStoreName = "online-aggregator-store";
        this.interval = 10;
        this.sessionInactivityGap = 60;
    }
}