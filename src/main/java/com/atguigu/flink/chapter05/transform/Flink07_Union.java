package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink07_Union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> one = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> two = env.fromElements(10, 20, 30, 40, 50, 6);
        one.union(two)
            .map(new MapFunction<Integer, String>() {
                @Override
                public String map(Integer value) throws Exception {
                    return value + ">";
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
union;
1. union 的流的数据类型必须一致
2. union可以多个流同时进行
 
 */
