package com.atguigu.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink01_Map {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .fromElements(1, 2, 3, 4, 5)
            /*.map(new MapFunction<Integer, Integer>() {
                @Override
                public Integer map(Integer value) throws Exception {
                
                    return value * value;
                }
            })*/
            .map(value -> value * value)
            .print();
    
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
