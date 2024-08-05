package com.example.service;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.dto.Customer;

import jakarta.annotation.PostConstruct;

@Service
public class KafkaServices {
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private List<Customer> customers = null;

    @PostConstruct
    public void loadCustomers(){
        this.customers = IntStream.rangeClosed(1, 100).mapToObj(
            i -> Customer.builder()
                        .id(i)
                        .name("Employee "+i)
                        .email("emp"+i+"@gmail.com")
                        .salary(new Random().nextInt(100000))
                        .build()
        ).toList();
    }

    public void sendMessageToSubscriber(String message){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("kafka-test-topic-v2",message);

        future.whenComplete((result, error) -> {
            if(error == null){
                System.out.println("Send message [ "+ message +" ]");
                System.out.println("Offset number [ "+ result.getRecordMetadata().offset() +" ] | partition number [ "+ result.getRecordMetadata().partition() +" ]");

            }else{
                throw new RuntimeException("Unable to send message : "+error);
            }
        });
    }

    public List<Customer> getAllCustomers(){
        return customers;
    }

    public Customer getCustomerById(int id){
        return customers.stream()
                        .filter(csm -> id == csm.getId())
                        .findAny()
                        .orElseThrow(() -> new RuntimeException("Invalid id"));
    }

    public void sendEventToSubscriber(int id){
        Customer customer = getCustomerById(id);

        CompletableFuture<SendResult<String, Object>> send = kafkaTemplate.send("kafka-test-topic-v2",2,"customer-event", customer);

        send.whenComplete((result, error) -> {
            if(error == null){
                System.out.println("Send message [ "+ customer.toString() +" ]");
                System.out.println("Offset number [ "+ result.getRecordMetadata().offset() +" ] | partition number [ "+ result.getRecordMetadata().partition() +" ]");
            }else{
                throw new RuntimeException("Error occurs :"+error);
            }
        });
    }
}
