package com.kafka.kafkaTest;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class myPartition implements Partitioner {

    //对分区进行一些操作
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
        //获取分区数
        Integer integer = cluster.partitionCountForTopic(topic);
        //取模按轮询的方式发送
        return key.toString().hashCode()%integer;
    }

    //关闭资源
    @Override
    public void close() {

    }

    //可获取一些配置对配置进行一些操作
    @Override
    public void configure(Map<String, ?> map) {

    }
}
