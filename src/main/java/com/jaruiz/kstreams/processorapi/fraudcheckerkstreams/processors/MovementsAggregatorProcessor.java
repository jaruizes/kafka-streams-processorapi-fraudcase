package com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.processors;

import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.FraudCheckerUtils;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.model.Fraud;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.model.Movement;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;


@Log4j2
public class MovementsAggregatorProcessor implements Processor<String, Movement, String, Fraud> {
    
    private ProcessorContext<String, Fraud> context;
    private KeyValueStore<String, Fraud> kvStore;
    private Record<String, Fraud> newRecord;
    private Cancellable punctuator;
    protected String stateStoreName;
    protected int interval;
    protected int sessionInactivityGap;
    
    @Override
    public void init(ProcessorContext<String, Fraud> context) {
        this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore(stateStoreName);
        
        // clean key-value store
        try (KeyValueIterator<String, Fraud> iterator = this.kvStore.all()) {
            iterator.forEachRemaining(entry -> {
                this.kvStore.delete(entry.key);
            });
        }

        punctuator = this.context.schedule(Duration.ofSeconds(this.interval), PunctuationType.WALL_CLOCK_TIME, this::closeInactiveSessions);
    }
    
    @Override
    public void process(Record<String, Movement> currentOnlineMovement) {
        String key = currentOnlineMovement.key();

        log.info("Processing incoming movement with id <{}> and key <{}>", currentOnlineMovement.value().getId(), key);

        Fraud lastFraudStored = kvStore.get(key);

        if (kvStore.get(key) == null) {
            openNewSessionWindowWithTheNewMovement(currentOnlineMovement);
        } else {
            if (isSessionEndedForTheKey(key, currentOnlineMovement.timestamp())) {
                log.info("Detected session expired for movements associated to card <{}>. Closing session and emitting result", key);
                closeSessionAndEmitResult(key, lastFraudStored);
                openNewSessionWindowWithTheNewMovement(currentOnlineMovement);
            } else {
                updateFraudStored(currentOnlineMovement, lastFraudStored);
            }
        }
    }
    
    @Override
    public void close() {
        punctuator.cancel();
        closeInactiveSessions(Instant.now().toEpochMilli());
        try (KeyValueIterator<String, Fraud> iterator = this.kvStore.all()) {
            iterator.forEachRemaining(entry -> {
                this.kvStore.delete(entry.key);
            });
        }
    }

    private void closeInactiveSessions(Long currentTimestamp) {
        log.info("[{}] Scheduled action: looking for inactive sessions....", currentTimestamp);
        try (KeyValueIterator<String, Fraud> iterator = this.kvStore.all()) {
            while(iterator.hasNext()) {
                KeyValue<String, Fraud> entry = iterator.next();
                if (isSessionEndedForTheKey(entry.key, currentTimestamp)) {
                    log.info("\t -> Inactive session found for card: <{}>. Closing session and emitting result", entry.key);
                    closeSessionAndEmitResult(entry.key, entry.value);
                }
            }
        }
        log.info("[{}] Scheduled action: looking for inactive sessions finished", currentTimestamp);
    }
    
    private void updateFraudStored(Record<String, Movement> onlineRecord, Fraud lastFraudStored) {
        lastFraudStored.addMovement(onlineRecord.value());
        kvStore.put(onlineRecord.key(), lastFraudStored);
    }

    private boolean isSessionEndedForTheKey(String key, long referenceTimestamp) {
        Fraud fraudStored = this.kvStore.get(key);
        if (fraudStored != null) {
            long lastTimestamp = FraudCheckerUtils.iso8601ToEpoch(fraudStored.getLastMovementTimestamp());
            return (referenceTimestamp - lastTimestamp) / 1000 > sessionInactivityGap;
        }

        return false;
    }

    private void closeSessionAndEmitResult(String key, Fraud lastFraudStored) {
        // Clean key from storage
        this.kvStore.delete(key);

        // Forward fraud
        context.forward(new Record<>(key, lastFraudStored, Instant.now().toEpochMilli()));
    }

    private Fraud openNewSessionWindowWithTheNewMovement(Record<String, Movement> onlineRecord) {
        Fraud currentFraud = new Fraud();
        currentFraud.addMovement(onlineRecord.value());

        kvStore.put(onlineRecord.key(), currentFraud);

        return currentFraud;
    }
    
}