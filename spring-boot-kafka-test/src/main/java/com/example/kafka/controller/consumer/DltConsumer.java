package com.example.kafka.controller.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * @author JungleXia
 * @version 1.0
 * @title DltConsumer
 * @description 死信队列消费者
 * @date 2025/3/26 18:22:46
 */
@Component
public class DltConsumer {

    @DltHandler
    @KafkaListener(topics = "test_dlt.DLT", groupId = "zhTestGroup")
    public void dltTopic(ConsumerRecord<String, String> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("============死信队列=================");
        System.out.println(topic + " : " + record.value());
        ack.acknowledge();
    }
}
