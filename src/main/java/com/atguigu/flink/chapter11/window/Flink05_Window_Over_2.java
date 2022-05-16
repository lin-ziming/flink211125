package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/5/15 14:37
 */
public class Flink05_Window_Over_2 {
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
                              new WaterSensor("sensor_1", 6000L, 50),
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
        tEnv.createTemporaryView("sensor", table);
        
        /*tEnv.sqlQuery("select" +
                          " id, ts, vc, " +
//                          " sum(vc) over(partition by id order by ts rows between unbounded preceding and current row) sum_vc " +
//                          " sum(vc) over(partition by id order by ts rows between 1 preceding and current row) sum_vc " +
//                          " sum(vc) over(partition by id order by ts range between unbounded preceding and current row) sum_vc " +
                          " sum(vc) over(partition by id order by ts range between interval '2' second preceding and current row) sum_vc " +
                          "from sensor").execute().print();*/
        
        tEnv.sqlQuery("select" +
                          " id, ts, vc, " +
                          " sum(vc) over w sum_vc, " +
                          " max(vc) over w max_vc " +
                          "from sensor " +
                          "window w as(partition by id order by ts rows between unbounded preceding and current row)").execute().print();
        
        
    }
}
/*
 
 
 */