package com.atguigu.flink.chapter11.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author lzc
 * @Date 2022/5/16 9:50
 */
public class HiveDemo {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        tEnv.executeSql("create table person(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            " 'connector' = 'filesystem', " +
                            " 'path' = 'input/sensor.txt', " +
                            " 'format' = 'csv' " +
                            ")");
    
        // 1. 创建hiveCatalog对象
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "gmall", "input/");
        // 2. 注册hiveCatalog
        tEnv.registerCatalog("hive", hiveCatalog);
        
        // 3. 使用hiveCatalog
        tEnv.useCatalog("hive");
        tEnv.useDatabase("gmall");
        
        
        tEnv.sqlQuery("select * from default_catalog.default_database.person").execute().print();
        
        
    }
}
