package com.example.demo.topology;

import com.example.demo.domain.Greeting;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
@Slf4j
public class GreetingStreamsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        KStream<String, Greeting> greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), new JsonSerde<>()));

        greetingsStream.print(Printed.<String, Greeting>toSysOut().withLabel("greetingsStream"));

        KStream<String, Greeting> modifiedStream = greetingsStream
                .mapValues((readOnlyKey, value) -> new Greeting(value.getMessage(), value.getTimeStamp()));

        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), new JsonSerde<>()));

    }
}
