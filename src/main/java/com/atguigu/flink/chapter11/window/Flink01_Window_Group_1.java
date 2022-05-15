package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Author lzc
 * @Date 2022/5/15 14:37
 */
public class Flink01_Window_Group_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStream<WaterSensor> waterSensorStream =
            env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                              new WaterSensor("sensor_1", 2002L, 20),
                              new WaterSensor("sensor_2", 3000L, 30),
                              new WaterSensor("sensor_1", 4003L, 40),
                              new WaterSensor("sensor_1", 5000L, 50),
                              new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
        
//        GroupWindow win = Tumble.over(Expressions.lit(5).second()).on($("ts")).as("w");
//        GroupWindow win = Slide.over(lit(5).second()).every(lit(2).second()).on($("ts")).as("w");
        GroupWindow win = Session.withGap(lit(2).second()).on($("ts")).as("w");
        
        table
            .window(win)
            .groupBy($("id"), $("w"))
            .select($("id"), $("w").start().as("stt"), $("w").end().as("edt"), $("vc").sum().as("vc_sum"))
            .execute()
            .print();
        
    }
}
/*
统计每个sensor, 每5s的水位和

select id,w, sum(vc) from t group by id, w

*/