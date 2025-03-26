package com.example.kafka.controller.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class KafkaConsumer {

//    @KafkaListener(id="consumer1", topics = "test_topic", groupId = "zhTestGroup")
    public void cclTopic(ConsumerRecord<String, String> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("线程=" + Thread.currentThread().getId() + ", 数据key=" + record.key() + ", 数据value=" + record.value());
        System.out.println(record);
        //手动提交offset
        ack.acknowledge();
    }

    /**
     * 消费者抛出异常，用来测试死信队列
     * @param record    消息
     * @param ack   ack
     * @param topic 主题
     */
    @KafkaListener(id="consumer2", topics = "test_dlt", groupId = "zhTestGroup")
    @RetryableTopic(
        attempts = "3",
        backoff = @Backoff(delay = 1000, multiplier = 2),
        dltTopicSuffix = ".DLT"  // 死信主题后缀, 必须要配置RetryableTopic 才能进入私信队列
    )
    public void dltTopic(ConsumerRecord<String, String> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println(topic+"线程=" + Thread.currentThread().getId() + ", 数据key=" + record.key() + ", 数据value=" + record.value());
        System.out.println(record);
        // 测试死信队列
        throw new RuntimeException("dlt");
        // 手动提交offset
//         ack.acknowledge();
    }

    /**
     * 并发批量下拉数据，手动提交方式避免消息丢失
     * @param list 批量数据
     * @param ack  ack确认offset
     * @param topic 主题
     */
//    @KafkaListener(id="consumer3", topics = "test_list", groupId = "zhTestGroup")
    public void listen(List<String> list, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("线程=" + Thread.currentThread().getId() + " 本次批量拉取数量:" + list.size() + " 开始消费...." + topic);
        List<String> msgList = new ArrayList<>();
        for (String record : list) {
            Optional<?> kafkaMessage = Optional.ofNullable(record);
            // 获取消息
            kafkaMessage.ifPresent(o -> msgList.add(o.toString()));
        }
        if (msgList.size() > 0) {
            for (String msg : msgList) {
                System.out.println("线程=" + Thread.currentThread().getId() + " 开始消费["+topic+"]中的消息【" + msg + "】");
            }
            // 更新索引
            // updateES(messages);
        }
        //手动提交offset
        ack.acknowledge();
        msgList.clear();
        System.out.println("消费结束");
    }
}