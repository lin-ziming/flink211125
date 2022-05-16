package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/5/15 14:37
 */
public class Flink04_Window_Over_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStream<WaterSensor> waterSensorStream =
            env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                              new WaterSensor("sensor_1", 2002L, 20),
                              new WaterSensor("sensor_1", 3000L, 40),
                              new WaterSensor("sensor_1", 3000L, 50),
                              new WaterSensor("sensor_2", 5001L, 30),
                              new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
        // sum() over(partition by id order by ts rows between unbounded preceding and current row)
        // 如果不是计算topN, 则orderBy必须是时间的升序
        //                OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).following(Expressions.CURRENT_ROW).as("w");
        //  OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(1L)).following(Expressions.CURRENT_ROW).as("w");
        //          OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w");
        //          OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).following(CURRENT_RANGE).as("w");
        OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).as("w");
        
        table
            .window(w)
            .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")).as("vc_sum"))
            .execute()
            .print();
        
    }
}
/*
 
 
 */