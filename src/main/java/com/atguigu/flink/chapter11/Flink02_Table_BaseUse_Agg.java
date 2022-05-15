package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/5/15 9:02
 */
public class Flink02_Table_BaseUse_Agg {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 1.先获取一个流
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60)
            );
        
        // 2. 把流转成动态表
        // 2.1 先有个表的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 2.2 通过表的执行环境转成流
        Table table = tEnv.fromDataStream(waterSensorStream);
        
        // select id, sum(vc) as sum_vc from t group by id;
        Table resultTable = table
            .groupBy($("id"))
            .aggregate($("vc").sum().as("vc_sum"))
            .select($("id"), $("vc_sum"));
    
        DataStream<Tuple2<Boolean, Row>> s0 = tEnv
            .toRetractStream(resultTable, Row.class);
//        DataStream<Row> s1 = tEnv.toDataStream(resultTable);  // 不支持更新的表
        s0.print();
        System.out.println("xxxxxx");
    
        DataStream<Row> s1 = tEnv.toChangelogStream(resultTable);
        s1.print();
        
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }
}
// +I 表示新增 -U 撤回    +U更新后的数据