package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/5/15 10:15
 */
public class Flink04_Table_Kafka {
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
        
        // 连接Kafka
        
        tEnv
            .connect(
                new Kafka()
                    .version("universal")
                    .property("bootstrap.servers", "hadoop162:9092")
                    .property("group.id", "atguigu")
                    .topic("s1")
                    .startFromLatest()
            )
//            .withFormat(new Csv())
            .withFormat(new Json())
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        Table table = tEnv.from("sensor");
        
        Table result = table
            .where($("id").isEqual("sensor_1"))
            .select($("id"), $("vc"));
        
        
        // 创建一个表与kafka的topic进行关联
    
        tEnv
            .connect(
                new Kafka()
                    .version("universal")
                    .property("bootstrap.servers", "hadoop162:9092")
                    .topic("s2")
                    .sinkPartitionerRoundRobin()
            )
            //            .withFormat(new Csv().lineDelimiter(""))
            .withFormat(new Json())
            .withSchema(
                new Schema()
                    .field("id", DataTypes.STRING())
                    .field("vc", DataTypes.INT())
            )
            .createTemporaryTable("s2");
    
    
        result.executeInsert("s2");
        
        
    }
}
