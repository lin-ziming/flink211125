package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/15 10:46
 */
public class Flink08_SQL_Kafka_Upsert {
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
    
    
        Table result = tEnv.sqlQuery("select id, sum(vc) vc_sum from sensor group by id");
    
        tEnv.executeSql("create table s_out(" +
                            " id string, " +
                            " vc_sum int, " +
                            " primary key(id)not enforced" +
                            ")with( " +
                            "  'connector' = 'upsert-kafka', " +
                            "  'topic' = 's3', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'key.format' = 'json', " +
                            "  'value.format' = 'json' " +
                            ")");
    
        result.executeInsert("s_out");
        
    
    
    }
}
