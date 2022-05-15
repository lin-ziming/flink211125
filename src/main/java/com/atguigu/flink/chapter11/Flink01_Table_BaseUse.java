package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
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
public class Flink01_Table_BaseUse {
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
        // 3. 在动态表上执行连续查询, 得到一个新的动态表
        // select * from t where id='sensor_1'
       /* Table result = table
            .where("id='sensor_1'")
            .select(" id as id1, ts as ts1, vc");*/
        
        Table result = table
            .where($("id").isEqual("sensor_1"))
            .select($("id").as("id1"), $("ts").as("ts1"), $("vc"));
        
        result.printSchema();
        // 4. 把结果转成流
        //        DataStream<WaterSensor> resultStream = tEnv.toAppendStream(result, WaterSensor.class);
//        DataStream<Row> resultStream = tEnv.toAppendStream(result, Row.class);
        DataStream<Row> resultStream = tEnv.toDataStream(result); // 直接转成Row
        
        // 5. 输出
                resultStream.print();
       /* resultStream
            .map(new MapFunction<Row, String>() {
                @Override
                public String map(Row value) throws Exception {
                    Object obj = value.getField("id1");
                    return obj.toString();
                }
            })
            .print();
        */
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }
}
