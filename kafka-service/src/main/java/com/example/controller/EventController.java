package com.example.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.dto.MessageDto;
import com.example.service.KafkaServices;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
@RequestMapping("/producer")
public class EventController {

    @Autowired
    private KafkaServices kafkaServices;
    
    @PostMapping("/message")
    public ResponseEntity<?> sendMessage(@RequestBody MessageDto messageReq) {
        try {
            this.kafkaServices.sendMessageToSubscriber(messageReq.getMessage());

            return new ResponseEntity<>("Message send successfully", HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/customers")
    public ResponseEntity<?> getAllCustomers(){
        return new ResponseEntity<>(this.kafkaServices.getAllCustomers(), HttpStatus.OK);
    }

    @GetMapping("/customers/{id}")
    public ResponseEntity<?> getCustomerById(@PathVariable("id") int id){
        return new ResponseEntity<>(this.kafkaServices.getCustomerById(id), HttpStatus.OK);
    }

    @PostMapping("event/customers/{id}")
    public ResponseEntity<?> sendCustomerById(@PathVariable("id") int id){

        this.kafkaServices.sendEventToSubscriber(id);
        return new ResponseEntity<>("Object send successfully ", HttpStatus.OK);
    }
}
