package com.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
    
    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    // @KafkaListener(topics = "kafka-test-topic-v2", groupId = "demo-group-1")
    // public void consumeMessages(String messages){

    //     log.info("Consumer Received : {}",messages);
    // }

    @KafkaListener(topics = "kafka-test-topic-v2", groupId = "demo-group-1",
                    topicPartitions = {@TopicPartition(topic = "kafka-test-topic-v2", partitions = {"2"})})
    public void consumeMessages(ConsumerRecord<String,Object> record){

        String key = record.key();
        Object value = record.value();

        if(key.equals("customer-event")){
            log.info("Event Received : {}",value.toString());
        }else{
            throw new RuntimeException("Invalid event");
        }
        
    }
}
