package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.bean.WordLen;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/5/16 10:18
 */
public class Flink02_Table {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(
                new WaterSensor("hello a b abc atguigu", 1000L, 10),
                new WaterSensor("abc b abc atguigu", 1000L, 10),
                new WaterSensor("hello  atguigu", 1000L, 10),
                new WaterSensor("hello  atguigu world", 1000L, 10)
            );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(waterSensorStream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 注册自定义函数
        // 2. 可以在tableapi中使用
        // 2.1 使用内联的方式
        // 2.2先注册再使用
        tEnv.createFunction("my_split", MySplit.class);
        table
            .joinLateral(Expressions.call("my_split", $("id")))
            .select($("id"), $("word"), $("len"))
            .execute()
            .print();
        // 3. 在sql中使用
        
    }
    
   /* @FunctionHint(output = @DataTypeHint("row<word string, len int>"))
    public static class MySplit extends TableFunction<Row> {
        public void eval(String line){
            // collect方法调用几次, 就表示这个line会制成几行
            for (String word : line.split(" ")) {
                collect(Row.of(word, word.length()));
            }
        }
    }*/
   public static class MySplit extends TableFunction<WordLen> {
       public void eval(String line){
           // collect方法调用几次, 就表示这个line会制成几行
           for (String word : line.split(" ")) {
               collect(new WordLen(word, word.length()));
           }
       }
   }
}
/*
hello  atguigu

        hello  5
        atguigu 7
hello  atguigu world
        hello  5
        atguigu 7
        world 5
        
   ....
 */