package com.kafka.kafkaTest;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

import java.util.Map;

public class myTimeInterceptor implements ProducerInterceptor<String,String> {

    int success;
    int error;

    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord<String,String> producerRecord) {
        //需求1:消息发送前将时间戳信息加到消息value的最前部
        //1.取出数据value
        String value = producerRecord.value();
        //因为producerRecord里面没有set方法,所以创建新的ProducerRecord对象
        ProducerRecord producerRecordNew = new ProducerRecord(producerRecord.topic(),
                producerRecord.partition(),producerRecord.key(),System.currentTimeMillis()+","+value);
        return producerRecordNew;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //需求2:消息发送后更新成功发送消息数或失败发送消息数
        if (recordMetadata!=null){
            success++;
        }else {
            error++;
        }

    }

    @Override
    public void close() {
        System.out.println("发送成功"+success);
        System.out.println("发送失败"+error);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }






}
