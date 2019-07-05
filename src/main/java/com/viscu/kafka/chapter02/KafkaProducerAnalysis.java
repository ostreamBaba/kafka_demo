package com.viscu.kafka.chapter02;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author ostreamBaba
 * @date 2019/07/05 下午3:33
 */

@Slf4j
public class KafkaProducerAnalysis {

    public static final String brokerList = "localhost:9092";

    public static final String topic = "topic-demo";

    public static Properties initConfig(){
        Properties config = new Properties();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        // 对于可重试的异常 设置retries参数 在规定次数内重试并恢复正常则不会抛出异常
        config.put(ProducerConfig.RETRIES_CONFIG, 10);
        return config;
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(initConfig());

        // 若在配置文件中没有指定key、value的序列化工具
        // KafkaProducer<String, String> producer1 = new KafkaProducer<>(initConfig(), new StringSerializer(), new StringSerializer());

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello, kafka");

        // 发送消息有三种模式： 发后即忘 同步与异步
        try {
            // 1.发后即忘
            //producer.send(record);
            // 2.同步 send()本身是异步方法 不过调用get()后会阻塞在get()方法上
            //producer.send(record).get();
            // 3.同步 可以获取一个RecordMetadata对象 包含发送消息的一些元数据信息
           /* Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());*/
            // 4 异步
            producer.send( record, (metadata1, exception) -> {
                // metadata与exception是互斥的
                if(exception != null){
                    log.error("The producer: " + exception.getMessage());
                }else {
                    System.out.println(metadata1.topic() + "-" + metadata1.partition() + ":" + metadata1.offset());
                }
            });
        } catch (Exception e){
            log.error("The producer : " + e.getMessage());
        } finally {
            producer.close();
        }
    }

    @Test
    public void test(){
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        int i = 0;
        try{
            while (i < 100){
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "msg" + (i++));
                try {
                    producer.send(record).get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("The producer:" + e.getMessage());
                }
            }
        } finally {
            producer.close();
        }
    }


    @Test
    public void test1(){
        Properties properties = initConfig();
        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("viscu").address("kafka").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
        try {
            producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }


}
