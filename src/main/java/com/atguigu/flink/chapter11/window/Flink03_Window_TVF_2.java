package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.Person;
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
public class Flink03_Window_TVF_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStream<Person> waterSensorStream =
            env
                .fromElements(
                    new Person("a1", "b1", "c1", "d1", 1000L, 10),
                    new Person("a1", "b1", "c2", "d2", 2000L, 20),
                    new Person("a1", "b3", "c2", "d2", 3000L, 30),
                    new Person("a2", "b2", "c1", "d1", 4000L, 40),
                    new Person("a2", "b1", "c1", "d1", 5000L, 50),
                    new Person("a2", "b2", "c1", "d1", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<Person>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
                );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(waterSensorStream, $("a"),$("b"),$("c"),$("d"), $("ts").rowtime(), $("vc"));
        tEnv.createTemporaryView("sensor", table);
       /* tEnv
            .sqlQuery("select" +
                          " ''  id,"+
                          " window_start, " +
                          " window_end, " +
                          " sum(vc) vc_sum " +
                          "from table( tumble(table sensor, descriptor(ts), interval '5' second) )" +
                          "group by window_start, window_end " +
                
                          "union " +
                
                          "select" +
                          " id, " +
                          " window_start, " +
                          " window_end, " +
                          " sum(vc) vc_sum " +
                          "from table( tumble(table sensor, descriptor(ts), interval '5' second) )" +
                          "group by id, window_start, window_end " +
                          "")
            .execute()
            .print();*/
    
        /*tEnv
            .sqlQuery("select" +
                          " id, " +
                          " window_start, " +
                          " window_end, " +
                          " sum(vc) vc_sum " +
                          "from table( tumble(table sensor, descriptor(ts), interval '5' second) )" +
                          "group by window_start, window_end, grouping sets( (id),()  ) " +
                          "")
            .execute()
            .print();*/
        
        /*tEnv
            .sqlQuery("select " +
                          "a, b, c, d, sum(vc) sum_vc " +
                          "from table( tumble( table sensor, descriptor(ts), interval '5' second) ) " +
                          "group by window_start, window_end, grouping sets(" +
                          " (a, b, c, d)," +
                          " (a, b, c   )," +
                          " (a, b      )," +
                          " (a        ),"  +
                          " (         )"  +
                          ")")
            .execute()
            .print();*/
    
        /*tEnv
            .sqlQuery("select " +
                          "a, b, c, d,window_start, window_end, sum(vc) sum_vc " +
                          "from table( tumble( table sensor, descriptor(ts), interval '5' second) ) " +
                          "group by window_start, window_end, rollup(a,b,c,d)")
            .execute()
            .print();*/
    
        tEnv
            .sqlQuery("select " +
                          "a, b, c, d,window_start, window_end, sum(vc) sum_vc " +
                          "from table( tumble( table sensor, descriptor(ts), interval '5' second) ) " +
                          "group by window_start, window_end, cube(a,b,c,d)")
            .execute()
            .print();
      
    
    }
}
/*
统计每个sensor, 每5s的水位和

select id,w, sum(vc) from t group by id, w

*/