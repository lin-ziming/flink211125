package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author lzc
 * @Date 2022/5/16 10:18
 */
public class Flink03_Agg {
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
        
        // 2.2先注册再使用
        tEnv.createTemporaryFunction("my_avg", MyAvg.class);
       /* table
            .groupBy($("id"))
            .select($("id"), call("my_avg", $("vc")))
            .execute()
            .print();*/
        // 3. 在sql中使用
        tEnv
            .sqlQuery("select id, my_avg(vc) from sensor group by id").execute().print();
        
    }
    
    public static class SumCount {
        public Integer sum = 0;
        public Long count = 0L;
        
    }
    
    public static class MyAvg extends AggregateFunction<Double, SumCount> {
        
        // 返回最终聚合的结果
        @Override
        public Double getValue(SumCount acc) {
            return acc.sum * 1.0 / acc.count;
        }
        
        // 创建累加器
        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }
        
        // 方法名必须是:accumulate
        // 第一个参数必须是: 累加器
        // 其他根据实际情况
        public void accumulate(SumCount sc, Integer vc) {
            sc.sum += vc;
            sc.count++;
        }
        
    }
    
}
