package com.jaruiz.kstreams.processorapi.fraudchecker.processors;

import com.jaruiz.kstreams.processorapi.fraudchecker.model.Movement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class MapKeyProcessor implements Processor<String, Movement, String, Movement> {

    private static final int ONLINE_MOVEMENT = 3;
    private ProcessorContext<String,Movement> context;

    public MapKeyProcessor() {}

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        context.forward(new Record<>(record.value().getCard(), record.value(), record.timestamp()));
    }

    @Override
    public void close() {

    }

}
