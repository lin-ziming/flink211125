package com.atguigu.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink05_Shuffle {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        env
            .fromElements(1, 2, 3, 4, 5, 6)
//            .shuffle()
//            .rebalance()
            .rescale()
            .print();
            
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
 
 
 
 */
