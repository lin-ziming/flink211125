package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/15 10:46
 */
public class Flink06_SQL_File {
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
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/sensor.txt', " +
                            " 'format' = 'csv' " +
                            ")");
    
    
        Table result = tEnv.sqlQuery("select * from sensor where id='sensor_1'");
    
        // 2. 通过ddl方式建表(动态表), 与文件关联
        tEnv.executeSql("create table s_out(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/a.txt', " +
                            " 'format' = 'csv' " +
                            ")");
    
//        result.executeInsert("s_out");
    
        tEnv.executeSql("insert into s_out select * from " + result);
    
    
    }
}
