package com.example.kafka.controller;

import com.example.kafka.controller.producer.KafkaProducer;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MemberProducerController {

    @Resource
    private KafkaProducer kafkaProducer;

    @GetMapping("/publish/{topicName}")
    public String sendMsg(@PathVariable String topicName) {
        kafkaProducer.send(topicName);
        return "success";
    }
}