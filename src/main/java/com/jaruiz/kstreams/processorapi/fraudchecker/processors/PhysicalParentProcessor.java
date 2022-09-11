package com.jaruiz.kstreams.processorapi.fraudchecker.processors;

import com.jaruiz.kstreams.processorapi.fraudchecker.model.Movement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class PhysicalParentProcessor implements Processor<String, Movement, String, Movement> {

    private ProcessorContext<String,Movement> context;

    public PhysicalParentProcessor() {}

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        context.forward(record);
    }

    @Override
    public void close() {}

}
