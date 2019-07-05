package com.viscu.kafka.chapter02;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author ostreamBaba
 * @date 2019/07/05 下午11:25
 */

// 自定义拦截器

public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String>{

    private AtomicInteger sendSuccess = new AtomicInteger(0);

    private AtomicInteger sendFailure = new AtomicInteger(0);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
    }


    // metadata与exception是互斥的
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            sendSuccess.getAndIncrement();
        }else {
            sendFailure.getAndIncrement();
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess.get() / (sendSuccess.get() + sendFailure.get());
        System.out.println("[INFO] the successRatio=" + successRatio * 100 + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    public static final CountDownLatch LATCH = new CountDownLatch(100);

    public static final LongAdder ADDER = new LongAdder();

    static class Test extends Thread{
        @Override
        public void run() {
            for (int i = 0; i < 10000; i++) {
                ADDER.increment();
            }
            LATCH.countDown();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Test[] tests = new Test[100];
        for (int i = 0; i < 100; i++) {
            tests[i] = new Test();
        }
        for (int i = 0; i < 100; i++) {
            tests[i].start();
        }
        LATCH.await();
        System.out.println(ADDER);
    }



}
