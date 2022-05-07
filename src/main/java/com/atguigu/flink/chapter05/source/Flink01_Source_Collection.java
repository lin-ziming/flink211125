package com.atguigu.flink.chapter05.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 10:35
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
    
//        DataStreamSource<String> stream = env.fromCollection(Arrays.asList("a", "b", "c", "c"));
        DataStreamSource<String> stream = env.fromElements("a", "b", "c", "d");
        stream.print();
    
    
        env.execute();
        
    }
}
