package com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.processors;

import com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.model.Movement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class MergeProcessor implements Processor<String, Movement, String, Movement> {

    private ProcessorContext<String,Movement> context;

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        context.forward(record);
        context.commit();
    }

    @Override
    public void close() {

    }

}
