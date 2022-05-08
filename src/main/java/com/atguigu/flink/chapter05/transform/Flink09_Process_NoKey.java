package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/5/7 15:33
 */
public class Flink09_Process_NoKey {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994001L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        env
            .fromCollection(waterSensors)
            .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                int sum = 0;  // 一种状态
                
                // 流中每来一个元素, 这个方法执行一次
                @Override
                public void processElement(WaterSensor value,  //
                                           Context ctx,
                                           Collector<WaterSensor> out) throws Exception {
                    sum += value.getVc();
                    
                    value.setVc(sum);
                    out.collect(value);
                }
            }).setParallelism(2)
            .print().setParallelism(2);
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
 
 
 */