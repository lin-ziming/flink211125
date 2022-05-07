package com.atguigu.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink06_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");
        
        ConnectedStreams<Integer, String> streams = intStream.connect(stringStream);
        
        streams
            .map(new CoMapFunction<Integer, String, String>() {
                @Override
                public String map1(Integer value) throws Exception {
                    return value + ">>>";
                }
                
                @Override
                public String map2(String value) throws Exception {
                    return value + "???";
                }
            })
            
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
 connect:
 1. connect只能连接两个流
 2. 两个流的类型可以不一致
 
 */
