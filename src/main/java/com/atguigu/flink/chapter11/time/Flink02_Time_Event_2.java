package com.atguigu.flink.chapter11.time;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author lzc
 * @Date 2022/5/15 14:04
 */
public class Flink02_Time_Event_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
       
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int, " +
                            " et as to_timestamp_ltz(ts, 3), " +
                            " watermark for et as et - interval '3' second " +  // 事件时间类型必须是timestamp(3)
                            ")with(" +
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's1', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'atguigu', " +
                            "  'scan.startup.mode' = 'latest-offset', " +
                            "  'format' = 'csv'" +
                            ")");
    
        Table table = tEnv.from("sensor");
        table.printSchema();
    
        tEnv.sqlQuery("select * from sensor").execute().print();
    }
}
/*
bigint          timestamp(3)
 */