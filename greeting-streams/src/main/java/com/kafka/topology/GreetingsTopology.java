package com.kafka.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology() {

        // Topology include: source processor -> stream processor -> sink processor
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> greetingStream = streamsBuilder
                .stream(GREETINGS
//                        , Consumed.with(Serdes.String(), Serdes.String())
                );

        KStream<String, String> greetingSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH
//                        , Consumed.with(Serdes.String(), Serdes.String())
                );

        var mergedStream = greetingStream.merge(greetingSpanishStream);

        mergedStream.print(Printed.<String, String>toSysOut().withLabel("greetingStream"));

        KStream<String, String> modifiedStream = greetingStream
                .filter((k, v) -> v.length() > 5)
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
                .peek((key, value) ->
                        log.info("key-value:  " + key + ":" + value));

//                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))
//                .flatMap((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
//                            return newValues
//                                    .stream()
//                                    .map(v -> KeyValue.pair(key, v.toUpperCase()))
//                                    .collect(Collectors.toList());

//                .flatMapValues((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
//
//                    return newValues
//                            .stream()
//                            .map(String::toUpperCase)
//                            .collect(Collectors.toList());
//                });


        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));


        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();

    }
}
