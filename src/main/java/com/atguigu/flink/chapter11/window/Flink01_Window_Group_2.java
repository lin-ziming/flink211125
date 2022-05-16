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
public class Flink01_Window_Group_2 {
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
        tEnv.createTemporaryView("sensor", table);
        
     
        /*tEnv.sqlQuery("select " +
                          "id, " +
                          "tumble_start(ts, interval '5' second) stt, " +
                          "tumble_end(ts, interval '5' second) edt, " +
                          "sum(vc) vc_sum " +
                          "from sensor " +
                          "group by id, tumble(ts, interval '5' second)")
            .execute()
            .print();*/
        
         /*tEnv.sqlQuery("select " +
                          "id, " +
                         " hop_start(ts, interval '2' second, interval '5' second) stt, " +
                         " hop_end(ts, interval '2' second, interval '5' second) edt, " +
                          "sum(vc) vc_sum " +
                          "from sensor " +
                          "group by id, HOP(ts, interval '2' second, interval '5' second)")
            .execute()
            .print();*/
         
         tEnv.sqlQuery("select " +
                          "id, " +
                          "session_start(ts, interval '2' second) stt, " +
                          "session_end(ts, interval '2' second) edt, " +
                          "sum(vc) vc_sum " +
                          "from sensor " +
                          "group by id, session(ts, interval '2' second)")
            .execute()
            .print();
        
    }
}
/*
统计每个sensor, 每5s的水位和

select id,w, sum(vc) from t group by id, w

*/