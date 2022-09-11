package com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.processors;

import com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.model.Movement;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

@Log4j2
public class OnlineParentProcessor implements Processor<String, Movement, String, Movement> {

    private ProcessorContext<String,Movement> context;

    public OnlineParentProcessor() {}

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        context.forward(record);
    }

    @Override
    public void close() {

    }

}
