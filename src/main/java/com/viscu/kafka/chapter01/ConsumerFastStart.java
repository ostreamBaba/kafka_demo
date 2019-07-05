package com.viscu.kafka.chapter01;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author ostreamBaba
 * @date 2019/06/07 下午10:07
 */

public class ConsumerFastStart {

    public static final String BROKER_LIST = "localhost:9092";

    public static final String TOPIC = "topic-demo";

    // 配置消费组
    public static final String GROUP_ID = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common." +
                "serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common." +
                "serialization.StringDeserializer");
        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("group.id", GROUP_ID);

        // 创建一个消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅相关主题
        consumer.subscribe(Collections.singletonList(TOPIC));
        // 循环消费消息
        while (true){
            // 拉取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach((record)-> System.out.println(record.value()));
        }
    }

}
