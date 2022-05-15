package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/15 10:46
 */
public class Flink07_SQL_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 通过ddl方式建表(动态表), 与文件关联
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's1', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'atguigu', " +
                            "  'scan.startup.mode' = 'latest-offset', " +
                            "  'format' = 'csv'" +
                            ")");
    
    
        Table result = tEnv.sqlQuery("select * from sensor");
    
        tEnv.executeSql("create table s_out(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's2', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'format' = 'json', " +
                            "  'sink.partitioner' = 'round-robin' " +
                            ")");
    
        result.executeInsert("s_out");
        
    
    
    }
}
