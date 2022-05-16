package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lzc
 * @Date 2022/5/16 10:18
 */
public class Flink04_TableAgg {
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
                             new WaterSensor("sensor_1", 5000L, 50)
            );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(waterSensorStream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 注册自定义函数
        // 2. 可以在tableapi中使用
        // 2.1 使用内联的方式
        
        // 2.2先注册再使用
        tEnv.createTemporaryFunction("top2", Top2Vc.class);
        table
            .groupBy($("id"))
            .flatAggregate(call("top2", $("vc")))
            .select($("id"), $("rank"), $("score"))
            .execute()
            .print();
        
        // 3. 在sql中使用
        
        
    }
    
    public static class Top2Vc extends TableAggregateFunction<Result, FirstSecond> {
        
        // 创建累加器
        @Override
        public FirstSecond createAccumulator() {
            return new FirstSecond();
        }
        
        // 对要计算的数据进行累加
        public void accumulate(FirstSecond fs, Integer vc) {
            if (vc > fs.first) {
                fs.second = fs.first;
                fs.first = vc;
            } else if (vc > fs.second) {
                fs.second = vc;
            }
        }
        
        // 生成表
        // 约定
        public void emitValue(FirstSecond fs, Collector<Result> out) {
            out.collect(new Result("第一名", fs.first));
            if (fs.second > 0) {
                
                out.collect(new Result("第二名", fs.second));
            }
        }
        
        
    }
    
    public static class Result {
        
        public Result(String rank, Integer score) {
            this.rank = rank;
            this.score = score;
        }
        
        public String rank;
        public Integer score;
    }
    
    public static class FirstSecond {
        public Integer first = 0;
        public Integer second = 0;
    }
    
}
/*
计算每个传感器的top2的水位
new WaterSensor("sensor_1", 1000L, 10),
    
    名次    水位值
    第一名   10
 new WaterSensor("sensor_1", 2000L, 20),
 
    名次    水位值
    第一名   20
    第一名   10
 new WaterSensor("sensor_1", 4000L, 40),
     名次    水位值
    第一名   40
    第一名   20
    
  ....

有没有聚合? 有
有没有制表? 有


 */
