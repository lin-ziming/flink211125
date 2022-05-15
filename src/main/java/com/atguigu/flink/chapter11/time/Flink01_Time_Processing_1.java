package com.atguigu.flink.chapter11.time;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/15 14:04
 */
public class Flink01_Time_Processing_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
       
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table sensor(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int, " +
                            " pt as proctime()" +  // 添加处理时间
                            ")with(" +
                            " 'connector' = 'kafka', " +
                            "  'topic' = 's1', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'atguigu', " +
                            "  'scan.startup.mode' = 'latest-offset', " +
                            "  'format' = 'csv'" +
                            ")");
        
        tEnv.sqlQuery("select * from sensor").execute().print();
    }
}
