package com.kafka.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class GreetingDeserializer implements Deserializer<Greeting> {

    ObjectMapper mapper;

    public GreetingDeserializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Greeting deserialize(String s, byte[] bytes) {
        try {
            return mapper.readValue(s, Greeting.class);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: {}, {} ", s, e.getMessage());
        }
        return new Greeting();
    }
}
