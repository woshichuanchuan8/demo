package com.kafka.kafkaTest;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class kafkaProducer {
    public static void main(String[] args) {

        //1.创建kafka生产者的配置信息
        Properties props=new Properties();

        //2.指定连接的kafka集群
        props.put("bootstrap.servers","hadoop102:9092");
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //3.Ack应答级别
        props.put("acks","all");

        //4.重试次数
        props.put("retries",1);

        //5批次大小(16384是16k)
        props.put("batch.size",16384);

        //6.等待时间,默认1ms
        props.put("linger",1);

        //7.缓冲区大小
        props.put("buffer.memory",33554432);

        //8.key和value的序列化类
        props.put("key.serializer","org.apache.kafka.common.seriallzation.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.seriallzation.StringSerializer");

        //============如何使用自定义分区时需要新建分区类,设置到属性中===========
        //自定义组件一般设置全类名
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafka.kafkaTest.myPartition");

        //============如何使用自定义拦截器类,设置到属性中===========
        //自定义组件一般设置全类名
        List list= new ArrayList();
        //可以定义多个拦截器形成拦截器链,注意添加顺序即可
        list.add("com.kafka.kafkaTest.myTimeInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);

        //9.创建生产者对象
        KafkaProducer producer = new KafkaProducer<>(props);

        //10.1======普通生产者发送数据=====
        /*for (int i = 0; i <10 ; i++) {
            producer.send(new ProducerRecord("first","atguigu"+i));
        }*/

        //10.2=============高级者发送数据带回调函数=============
        for (int i = 0; i <10 ; i++) {
            producer.send(new ProducerRecord("first", "atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (null==exception){
                        //成功,打印返回的分区和偏移量
                        System.out.println(recordMetadata.partition()+"---"+recordMetadata.offset());
                    }else {
                        //失败
                        System.out.println("发送失败");
                    }
                }
            });
        }


        //11.关闭资源,因为上面默认是等待1ms或者文件大小写到16k的时候发送,如果不关闭资源
        //循环10次很快到不了1ms文件大小也到不了16k,内存直接就删除了,就不会发送
        //这里的关闭实际生产中生产者是一直接收消息的,while(true)一样,所以关闭就在try--catch中的finnaly里面close
        producer.close();

    }
}


//从电话文件中读取电话,用户名信息数据
//模拟生成通话开始时间和通话时长
//将组成生成好的通话文件写入数据文件中xx.log
//在frum中配置上面产生的log.kafka集群,配置主题topic
// 在kafka中创建topic,分区和副本(以上执行就开始生产数据了)
//创建消费者设置参数开始消费 consumer.poll()
