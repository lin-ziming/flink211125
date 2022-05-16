package com.atguigu.flink.chapter12;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/16 14:04
 */
public class TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 1. 建立动态表, 与数据源关联
        tEnv.executeSql("create table ub(" +
                            " user_id bigint, " +
                            " item_id bigint, " +
                            " category_id bigint, " +
                            " behavior string, " +
                            " ts bigint, " +
                            " et as to_timestamp_ltz(ts, 0)," +
                            " watermark for et as et - interval '3' second" +
                            ")with(" +
                            "   'connector'= 'filesystem'," +
                            "   'path'= 'input/UserBehavior.csv'," +
                            "   'format'= 'csv'" +
                            ")");
        
        // 2. 开窗统计每个商品的点击量
        Table t1 = tEnv.sqlQuery("select " +
                                        " item_id, " +
                                        " window_start stt, " +
                                        " window_end edt, " +
                                        " count(*) ct " +   // count(*) count(id)  sum(1)
                                        " from table( hop( table ub, descriptor(et), interval '1' hour, interval '2' hour) ) " +
                                        " where behavior='pv' " +
                                        " group by item_id, window_start, window_end ");
        tEnv.createTemporaryView("t1", t1);
        // 3. 给点击量排名
        Table t2 = tEnv.sqlQuery("select" +
                                        "   *, " +
                                        "   row_number() over(partition by edt order by ct desc) rn" +  // rank row_number dense_rank
                                        " from t1");
        tEnv.createTemporaryView("t2", t2);
        
        
        
        // 4. 取top3
        Table result = tEnv.sqlQuery("select " +
                                        " edt w_end, " +
                                         " item_id, " +
                                         " ct item_count, " +
                                         " rn rk " +
                                        "from t2 " +
                                        "where rn <= 3");
    
        // 5. 写出到Mysql中
        // 5.1 建立动态表与Mysql关联
        tEnv.executeSql("CREATE TABLE `hot_item` ( " +
                            "  `w_end` timestamp , " +
                            "  `item_id` bigint, " +
                            "  `item_count` bigint, " +
                            "  `rk` bigint, " +
                            "  PRIMARY KEY (`w_end`,`rk`)not enforced " +
                            ")with(" +
                            "   'connector' = 'jdbc'," +
                            "   'url' = 'jdbc:mysql://hadoop162:3306/flink_sql?useSSL=false'," +
                            "   'table-name' = 'hot_item'," +
                            "   'username' = 'root', " +
                            "   'password' = 'aaaaaa' " +
                            ")  ");
        
        // 5.2 写出
        result.executeInsert("hot_item");
    
    
        
    }
}
