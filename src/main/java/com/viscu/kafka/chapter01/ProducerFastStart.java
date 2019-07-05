package com.viscu.kafka.chapter01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ostreamBaba
 * @date 2019/06/07 下午10:01
 * 生产者客户端代码
 */

public class ProducerFastStart {

    // 指定broker的服务地址
    public static final String BROKER_LIST = "localhost:9092";

    // 指定主题
    public static final String TOPIC = "topic-demo";

    public static void main(String[] args) {
        // 设置相关配置
        Properties properties = new Properties();
        // 设置key、value的序列化工具
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("bootstrap.servers", BROKER_LIST);

        // 创建kafka 生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 创建消息
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello, kafka");

        try {
            producer.send(record);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }

}
