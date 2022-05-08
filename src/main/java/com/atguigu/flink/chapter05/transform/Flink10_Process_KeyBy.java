package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author lzc
 * @Date 2022/5/7 15:33
 */
public class Flink10_Process_KeyBy {
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
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                HashMap<String, Integer> idToSum = new HashMap<>(); //
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    String key = ctx.getCurrentKey();
                    Integer sum = idToSum.getOrDefault(key, 0);
                    sum += value.getVc();
                    idToSum.put(key, sum);
    
                    out.collect(key + " 的水位和: " + sum);
                    
                    
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
 
 
 */