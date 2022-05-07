package com.atguigu.flink.chapter05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/5/7 11:05
 */
public class Flink03_Source_Kafka_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092");
        props.setProperty("group.id", "atguigu");
//        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), props));
        DataStreamSource<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer<>("s1", new JSONKeyValueDeserializationSchema(false), props));
        stream.map(x -> x.get("value")).print();
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
