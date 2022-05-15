package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/5/15 10:15
 */
public class Flink03_Table_File {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
    
    
        tEnv
            .connect(new FileSystem().path("input/sensor.txt"))
            .withFormat(new Csv().lineDelimiter("\n").fieldDelimiter(','))
            .withSchema(schema)
            .createTemporaryTable("sensor");
    
        Table table = tEnv.from("sensor");
    
        Table resultTable = table
            .where($("id").isEqual("sensor_1"))
            .select($("id"), $("ts"), $("vc"));
        
        //resultTable.execute().print();
        
        // 建立一个新的表与文件关联
        tEnv
            .connect(new FileSystem().path("input/abc.txt"))
            .withFormat(new Csv().fieldDelimiter(','))
            .withSchema(schema)
            .createTemporaryTable("abc");
        
        resultTable.executeInsert("abc");
        
        
    

    }
}
