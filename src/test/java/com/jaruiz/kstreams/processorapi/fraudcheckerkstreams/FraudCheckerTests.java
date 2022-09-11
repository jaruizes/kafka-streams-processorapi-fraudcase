package com.jaruiz.kstreams.processorapi.fraudcheckerkstreams;

import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.model.Fraud;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.model.Movement;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.serializers.JsonDeserializer;
import com.jaruiz.kstreams.processorapi.fraudcheckerkstreams.serializers.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@SpringBootTest
public class FraudCheckerTests {

    public static final String MOVEMENTS_TOPIC = "movements";
    public static final String FRAUD_TOPIC = "fraud-cases";
    public static final int ATM_MOVEMENT = 1;
    public static final int MERCHANT_MOVEMENT = 2;
    public static final int ONLINE_MOVEMENT = 3;

    private FraudChecker fraudCheckerProcessor = new FraudChecker();
    private StreamsBuilder streamsBuilder;
    private DateTimeFormatter formatter;
    private final Serde<Movement> movementSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Movement.class));
    private final Serde<Fraud> fraudSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Fraud.class));

    @BeforeEach
    void setUp() {
        fraudCheckerProcessor = new FraudChecker();
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withLocale(Locale.UK).withZone(ZoneId.systemDefault());
        streamsBuilder = new StreamsBuilder();
    }

    @Test
    void checkNoFraud() {

        Instant instant = Instant.now();

        fraudCheckerProcessor.buildTopology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties(), Instant.now())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, Fraud> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> inputMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(10f).device("atm-1").site("site0").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant)).build(),
                    // fraud (c2: MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD && ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("atm-1").site("site1").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m2").amount(10f).device("atm-1").site("site2").origin(MERCHANT_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(75))).build(),
                    Movement.builder().id("m3").amount(10f).device("atm-1").site("site1").origin(ONLINE_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(95))).build(),
                    Movement.builder().id("m4").amount(10f).device("atm-2").site("site3").origin(ATM_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(5))).build(),
                    Movement.builder().id("m5").amount(10f).device("atm-1").site("site3").origin(MERCHANT_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(66))).build(),
                    // close window
                    Movement.builder().id("m6").amount(10f).device("atm-1").site("site0").origin(ATM_MOVEMENT).card("c5").createdAt(formatter.format(instant.plusSeconds(500))).build()
            );
            inputTopic.pipeValueList(inputMovements);

            // assertions
            List<Fraud> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(0, fraudCases.size());

        }

    }

    @Test
    void checkOnlineFraud() throws InterruptedException {

        Instant instant = Instant.now();

        fraudCheckerProcessor.buildTopology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties(), Instant.now())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, Fraud> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> inputMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(1000f).device("").site("site0").origin(ONLINE_MOVEMENT).card("c1").createdAt(formatter.format(instant)).build(),
                    // fraud (c2: MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD && ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("").site("site1").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(10))).build(),
                    Movement.builder().id("m2").amount(90f).device("").site("site2").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m3").amount(50f).device("").site("site2").origin(ONLINE_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(20))).build(),
                    Movement.builder().id("m4").amount(200f).device("").site("site3").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(30))).build(),
                    Movement.builder().id("m5").amount(90f).device("").site("site3").origin(ONLINE_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(40))).build(),
                    Movement.builder().id("m6").amount(100f).device("").site("site4").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(44))).build()
                    // close window
                    //Movement.builder().id("m7").amount(1000f).device("").site("site0").origin(ONLINE_MOVEMENT).card("c4").createdAt(formatter.format(instant.plusSeconds(545))).build()
            );
            inputTopic.pipeValueList(inputMovements);

            // Last record + INACTIVITY GAP
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(105));

            // assertions
            List<Fraud> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(1, fraudCases.size());
            Assertions.assertEquals(4, fraudCases.get(0).getMovements().size());
            Assertions.assertEquals(400L, fraudCases.get(0).getTotalAmount());
            Assertions.assertEquals(4, fraudCases.get(0).getSites().size());
            Assertions.assertEquals(0, fraudCases.get(0).getDevices().size());
            Assertions.assertEquals("ONLINE", fraudCases.get(0).getDescription());
            Assertions.assertEquals("c2", fraudCases.get(0).getCard());

        }

    }

    @Test
    void checkPhysicalFraud_physicalDevices() {

        Instant instant = Instant.now();

        fraudCheckerProcessor.buildTopology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, Fraud> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> physicalFraudMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(1000f).device("atm-0").site("site0").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant)).build(),
                    // fraud (c2,c3: ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("atm-1").site("site1").origin(ATM_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(5))).build(),
                    Movement.builder().id("m2").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m3").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(20))).build(),
                    Movement.builder().id("m4").amount(100f).device("shop-1").site("site3").origin(MERCHANT_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(30))).build(),
                    Movement.builder().id("m5").amount(90f).device("atm-1").site("site1").origin(ATM_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(40))).build()
                    // close window
                    //Movement.builder().id("m6").amount(1000f).device("shop-4").site("site4").origin(MERCHANT_MOVEMENT).card("c4").createdAt(formatter.format(instant.plusSeconds(545))).build()
            );
            inputTopic.pipeValueList(physicalFraudMovements);

            // Last record + INACTIVITY GAP
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(105));

            // assertions
            List<Fraud> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(2, fraudCases.size());

        }

    }

    @Test
    void checkPhysicalFraud_physicalMovements() {

        Instant instant = Instant.now();

        fraudCheckerProcessor.buildTopology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, Fraud> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> physicalFraudMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(1000f).device("atm-0").site("site0").origin(ATM_MOVEMENT).card("c0").createdAt(formatter.format(instant)).build(),
                    // fraud (c1: MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(5))).build(),
                    Movement.builder().id("m2").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m3").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(20))).build(),
                    Movement.builder().id("m4").amount(100f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(30))).build(),
                    Movement.builder().id("m5").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(40))).build()
                    // close window
                    //Movement.builder().id("m6").amount(1000f).device("shop-4").site("site4").origin(MERCHANT_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(545))).build()
            );
            inputTopic.pipeValueList(physicalFraudMovements);
            // Last record + INACTIVITY GAP
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(105));

            // assertions
            List<Fraud> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(1, fraudCases.size());
            Assertions.assertEquals(5, fraudCases.get(0).getMovements().size());
            Assertions.assertEquals(380, fraudCases.get(0).getTotalAmount());
            Assertions.assertEquals(0, fraudCases.get(0).getSites().size());
            Assertions.assertEquals(1, fraudCases.get(0).getDevices().size());
            Assertions.assertEquals("PHYSICAL", fraudCases.get(0).getDescription());
            Assertions.assertEquals("c1", fraudCases.get(0).getCard());

        }

    }

}
