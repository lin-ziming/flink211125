package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink03_FlatMap {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .fromElements(1, 2, 3, 4, 5)
            .flatMap(new RichFlatMapFunction<Integer, Integer>() {
                
                @Override
                public void flatMap(Integer value, Collector<Integer> out) throws Exception {
//                    out.collect(value * value);
//                    out.collect(value * value * value);
                    if (value % 2 == 0) {
                        out.collect(value);
                    }
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
