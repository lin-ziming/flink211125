package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink01_Map_Rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .fromElements(1, 2, 3, 4, 5)
            .map(new RichMapFunction<Integer, Integer>() {
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 每个并行度执行一次
                    System.out.println("Flink01_Map_Rich.open");
                }
    
                @Override
                public Integer map(Integer value) throws Exception {
                    System.out.println("Flink01_Map_Rich.map");
                    return value * value;
                }
    
                @Override
                public void close() throws Exception {
                    System.out.println("Flink01_Map_Rich.close");
                    // 每个并行度执行一次
                }
            }).setParallelism(2)
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
