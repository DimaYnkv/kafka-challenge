package com.cw.kafka.challenge;

import com.cw.kafka.challenge.serialization.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerTest {

    private static final String BOOTSTRAP_SERVERS= "localhost:9092";


    @Test
    public void createTestKafkaProducer(){
        String num1 = "c:/kafka_2.12-2.2.0/challenge/numeric1.json";
        String num2 = "c:/kafka_2.12-2.2.0/challenge/numeric2.json";
        String num3 = "c:/kafka_2.12-2.2.0/challenge/numeric3.json";
        String num4 = "c:/kafka_2.12-2.2.0/challenge/numeric4.json";
        String num5 = "c:/kafka_2.12-2.2.0/challenge/numeric5.json";
        String num6 = "c:/kafka_2.12-2.2.0/challenge/numeric6.json";

//         produceTestFiles(num1,"cellwise.kafka.challenge_1", 1);
//         produceTestFiles(num2,"cellwise.kafka.challenge_2", 1);
//         produceTestFiles(num3,"cellwise.kafka.challenge_33", 1);
//         produceTestFiles(num4,"cellwise.kafka.challenge_4", 1);
//         produceTestFiles(num5,"cellwise.kafka.challenge_5", 1);
//         produceTestFiles(num6,"cellwise.kafka.challenge_6", 1);
    }


    public void produceTestFiles(String filePath, String topic, int numOfMsgsToSend){
        ObjectMapper mapper = new ObjectMapper();
        try(InputStream is = new FileInputStream(filePath)) {
            JsonNode node = mapper.readTree(is);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
//            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

            Producer<String, JsonNode> kafkaProducer =
                    new KafkaProducer<>(
                            props,
                            new StringSerializer(),
                            new JsonSerializer()
                    );

            // Send a message

            for (int i = 0; i < numOfMsgsToSend; i++){
                System.out.println("send event #" + i);
                kafkaProducer.send(new ProducerRecord<String, JsonNode>(topic, null, node));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
