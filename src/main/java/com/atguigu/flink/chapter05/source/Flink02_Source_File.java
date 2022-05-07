package com.atguigu.flink.chapter05.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 10:35
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
    
        DataStreamSource<String> stream = env.readTextFile("hdfs://hadoop162:8020/words.txt");
    
        stream.print();
    
    
        env.execute();
        
    }
}
