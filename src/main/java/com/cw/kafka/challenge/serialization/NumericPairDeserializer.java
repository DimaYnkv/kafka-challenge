package com.cw.kafka.challenge.serialization;

import com.cw.kafka.challenge.model.NumericPair;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class NumericPairDeserializer implements Deserializer<NumericPair> {

    private ObjectMapper om = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

//        om.enable(DeserializationFeature.UNWRAP_ROOT_VALUE);
    }

    @Override
    public NumericPair deserialize(String s, byte[] bytes) {
        NumericPair vesEvent = null;
        try {
            vesEvent = om.readValue(bytes, NumericPair.class);
        } catch (Exception e) {
            System.out.println(String.format("Failed to deserialize vesEvent. exception: %s", e));
            return null;
        }

        return vesEvent;
    }

    @Override
    public void close() {

    }
}
