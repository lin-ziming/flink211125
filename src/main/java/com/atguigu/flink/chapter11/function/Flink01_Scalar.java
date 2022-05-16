package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author lzc
 * @Date 2022/5/16 10:18
 */
public class Flink01_Scalar {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor(null, 6000L, 60)
            );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(waterSensorStream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 注册自定义函数
        // 2. 可以在tableapi中使用
        // 2.1 使用内联的方式
        /*table
            .select($("id"), call(MyUpper.class, $("id")).as("id_upper"))
            .execute()
            .print();
        */
        // 2.2先注册再使用
        tEnv.createTemporaryFunction("my_upper", MyUpper.class);
       /* table
            .select($("id"), call("my_upper", $("id")).as("id_upper"))
            .execute()
            .print();*/
        
        // 3. 在sql中使用
        tEnv.sqlQuery("select " +
                          " id, " +
                          " my_upper(id) upper_id " +
                          " from sensor")
            .execute()
            .print();
    }
    
    public static class MyUpper extends ScalarFunction {
        // 约定方法
        // 方法名: eval
        public String eval(String s) {
            if (s != null) {
                return s.toUpperCase();
            }
            return null;
        }
        
    }
}
