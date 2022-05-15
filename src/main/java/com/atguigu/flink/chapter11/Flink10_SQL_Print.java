package com.atguigu.flink.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/15 9:02
 */
public class Flink10_SQL_Print {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 3000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
       
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
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
        // 2.2 通过表的执行环境转成流
        Table table = tEnv.from("sensor");
      
       /* tEnv.sqlQuery("select * from " + table).execute().print();  // 如果读的数据是无界, 为阻塞执行
        System.out.println("xxxxx");
        tEnv.sqlQuery("select id from " + table).execute().print();*/
        tEnv.executeSql("create table s1(" +
                            " id string, " +
                            " ts bigint, " +
                            " vc int" +
                            ")with(" +
                            " 'connector' = 'print' " +
                            ")");
        
        table.executeInsert("s1");
    
    
        tEnv.executeSql("create table s2(" +
                            " id string " +
                            ")with(" +
                            " 'connector' = 'print' " +
                            ")");
        tEnv.sqlQuery("select id from " + table ).executeInsert("s2");
       
       
        
    }
}
