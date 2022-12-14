package com.jaruiz.kstreams.processorapi.fraudchecker.processors;

import com.jaruiz.kstreams.processorapi.fraudchecker.model.Movement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class SplitProcessor implements Processor<String, Movement, String, Movement> {

    private static final int ONLINE_MOVEMENT = 3;
    private ProcessorContext<String,Movement> context;

    public SplitProcessor() {}

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        if (record.value().getOrigin() == ONLINE_MOVEMENT){
            context.forward(record, "online-movements");
        } else {
            context.forward(record, "physical-movements");
        }
    }

    @Override
    public void close() {

    }

}
