package com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.processors;

import com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.model.Fraud;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;

public class OnlineFilterProcessor implements Processor<String, Fraud, String, Fraud> {

    public final static float ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD = 200;
    public final static int MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD = 3;

    private ProcessorContext<String,Fraud> context;

    public OnlineFilterProcessor() {}

    @Override
    public void init(ProcessorContext<String, Fraud> context) {
        this.context = context;
     }

    @Override
    public void process(Record<String, Fraud> record) {
        if (isFraud(record.value())){
            context.forward(record);
        }
    }

    private boolean isFraud(Fraud fraud) {
       return fraud.getMovements().size() > MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD && fraud.getTotalAmount() > ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD;
    }

    @Override
    public void close() {}

}
