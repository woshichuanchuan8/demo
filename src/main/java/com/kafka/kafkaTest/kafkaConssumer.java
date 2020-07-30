package com.kafka.kafkaTest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class kafkaConssumer {
    public static void main(String[] args) {
        //1.创建kafka消费者的配置信息
        Properties props=new Properties();

        //2.指定连接的kafka集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        //3.开启自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        //4.自动提交延时
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        //5.key和value的反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.seriallzation.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.seriallzation.StringSerializer");

        //6.消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"biddata");

        //7.创建消费者
        KafkaConsumer conssumer = new KafkaConsumer<>(props);

        //8.订阅主题
        conssumer.subscribe(Arrays.asList("first","second"));


        System.out.println("git提交测试呢================");
        System.out.println("git提交测试呢================");
        System.out.println("git提交测试呢========11111========");
        System.out.println("git提交测试呢========2222========");
        System.out.println("git提交测试呢========you户========");


        //写个死循环阻塞,让他不停的拉取
        while(true) {
            //9.kafka采用的是拉取数据的模式这里拉取数据
            ConsumerRecords<String, String> consumerRecords = conssumer.poll(100);

            //10.解析并打印数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "---" + consumerRecord.value());
            }

        }



    }
}
