package com.jaruiz.kstreams.processorapi.fraudcheckerkstreams;


import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.extractors.MovementTimestampExtractor;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.model.Fraud;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.model.Movement;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.processors.*;
import com.paradigma.rt.streaming.processorapi.fraudcheckerkstreams.processors.*;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.serializers.JsonDeserializer;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.serializers.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FraudChecker {
    
    Serde<Movement> movementSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Movement.class));
    Serde<Fraud> fraudSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Fraud.class));

    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {
        
        Topology topology = streamsBuilder.build();

        // source (card movements)
        topology.addSource(Topology.AutoOffsetReset.EARLIEST, "Movement-Events-Source", new MovementTimestampExtractor(), Serdes.String().deserializer(), movementSerde.deserializer(), "movements");

        // stateless (generates new key and splits movements by origin)
        topology.addProcessor("Map-Key-Processor", MapKeyProcessor::new, "Movement-Events-Source");

        // Split: physical and online movements processed independently
        topology.addProcessor("Split-Processor", SplitProcessor::new, "Map-Key-Processor");
        topology.addProcessor("online-movements", OnlineParentProcessor::new, "Split-Processor");
        topology.addProcessor("physical-movements", PhysicalParentProcessor::new, "Split-Processor");

        // ONLINE
        StoreBuilder<KeyValueStore<String, Fraud>> onlineFraudStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("online-aggregator-store"), Serdes.String(), fraudSerde);
        topology.addProcessor("online-movements-aggregation", OnlineMovementsAggregatorProcessor::new, "online-movements")
                .addStateStore(onlineFraudStoreBuilder, "online-movements-aggregation");
        topology.addProcessor("online-movements-fraud-filter", OnlineFilterProcessor::new, "online-movements-aggregation");


        StoreBuilder<KeyValueStore<String, Fraud>> physicalFraudStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("physical-aggregator-store"), Serdes.String(), fraudSerde);
        topology.addProcessor("physical-movements-aggregation", PhysicalMovementsAggregatorProcessor::new, "physical-movements")
                .addStateStore(physicalFraudStoreBuilder, "physical-movements-aggregation");
        topology.addProcessor("physical-movements-fraud-filter", PhysicalFilterProcessor::new, "physical-movements-aggregation");
        // stateful (aggregate movements by key)


        topology.addProcessor("merge-movements", MergeProcessor::new,"online-movements-fraud-filter", "physical-movements-fraud-filter");
        topology.addSink("fraud-sink", "fraud-cases", Serdes.String().serializer(), fraudSerde.serializer(),"merge-movements");
    }
    
}
