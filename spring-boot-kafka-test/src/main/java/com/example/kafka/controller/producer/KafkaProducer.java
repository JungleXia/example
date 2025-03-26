package com.example.kafka.controller.producer;

import jakarta.annotation.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;


import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

@Component
public class KafkaProducer {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void send(String topicName) {
        System.out.println("kafka topicName：" + topicName);
        String message = "send msg: " + new Date().getTime();
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((result, ex) -> {

            if (ex != null) {
                System.out.println("Unable to send message=["
                    + message + "] due to : " + ex.getMessage());
            } else {
                System.out.println(topicName+ ": Sent message=[" + message +
                    "] with offset=[" + result.getRecordMetadata().offset() + "]" + "partition=[" + result.getRecordMetadata().partition() + "]");
            }
        });
    }
}