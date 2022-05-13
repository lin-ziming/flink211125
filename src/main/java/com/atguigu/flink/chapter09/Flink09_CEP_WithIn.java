package com.atguigu.flink.chapter09;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/5/13 11:09
 */
public class Flink09_CEP_WithIn {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        // 1. 先有数据流
        SingleOutputStreamOperator<WaterSensor> stream = env
            .readTextFile("input/sensor.txt")
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0],
                                           Long.parseLong(split[1]),
                                           Integer.parseInt(split[2])
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
            );
        
        
        // 2. 定义模式(规则)
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
            .<WaterSensor>begin("s1")
            .where(new SimpleCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return "sensor_1".equals(value.getId());
                }
            })
            .next("s2")   // 非确定性的松散连续
            .where(new SimpleCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return "sensor_2".equals(value.getId());
                }
            })
            .within(Time.seconds(2));
        
        
        // 3. 把模式或者规则作用到流上, 得到一个模式流
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        // 4. 从模式流中获取批到的数据, 会自动放入到一个流中
        SingleOutputStreamOperator<String> normal = ps
            .select(
                new OutputTag<String>("late") {},
                new PatternTimeoutFunction<WaterSensor, String>() {
                    @Override
                    public String timeout(Map<String, List<WaterSensor>> pattern,
                                          long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        
            );
    
        normal.print("normal");
    
        normal.getSideOutput(new OutputTag<String>("late") {}).print("late");
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
/*
三种数据:
1. 匹配到的数据  可以拿到

2. 每有匹配上的  直接抛弃, 也没有办法拿到

3. 超时数据  可以拿到

 */