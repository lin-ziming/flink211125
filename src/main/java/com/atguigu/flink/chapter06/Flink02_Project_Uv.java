package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink02_Project_Uv {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]),
                    data[3],
                    Long.valueOf(data[4])
                );
            })
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .keyBy(UserBehavior::getBehavior)
            .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                Set<Long> userIdSet = new HashSet<Long>();
                
                @Override
                public void processElement(UserBehavior value,
                                           Context ctx,
                                           Collector<Long> out) throws Exception {
                    /*int pre = userIdSet.size();
                    userIdSet.add(value.getUserId());
                    int post = userIdSet.size();
                    
                    if (post > pre) {
                        out.collect((long) userIdSet.size());
                        
                    }*/
                    if (userIdSet.add(value.getUserId())) {
                        out.collect((long) userIdSet.size());
                    }
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
