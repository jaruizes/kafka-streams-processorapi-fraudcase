package com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.processors;

import com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.model.Fraud;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class PhysicalFilterProcessor implements Processor<String, Fraud, String, Fraud> {
    
    public final static int MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD = 4;
    public final static int ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD = 1;

    private ProcessorContext<String,Fraud> context;

    @Override
    public void init(ProcessorContext<String, Fraud> context) {
        this.context = context;
     }

    @Override
    public void process(Record<String, Fraud> record) {
        Record<String,Fraud> newRecord = new Record<>(record.key(), record.value(), record.timestamp());
        if (record.value().getMovements().size() > MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD || record.value().getDevices().size() > ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD){
            context.forward(newRecord);
        }
    }

    @Override
    public void close() {}

}
