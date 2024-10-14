package com.kafka.serdes;

import com.kafka.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public Serde<Greeting> greetingSerdes() {
        return new GreetingSerdes();
    }

    public Serde<Greeting> greetingSerdesUsingGeneric() {

        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
