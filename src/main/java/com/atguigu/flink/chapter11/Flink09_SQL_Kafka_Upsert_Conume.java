package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/15 10:46
 */
public class Flink09_SQL_Kafka_Upsert_Conume {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table s_r(" +
                            " id string, " +
                            " vc_sum int, " +
                            " primary key(id)not enforced" +
                            ")with( " +
                            "  'connector' = 'upsert-kafka', " +
                            "  'topic' = 's3', " +
                            "  'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "  'properties.group.id' = 'abc', " +
                            "  'key.format' = 'json', " +
                            "  'value.format' = 'json' " +
                            ")");
        
        tEnv.sqlQuery("select * from s_r").execute().print();
        
        
    
    
      
        
    
    
    }
}
