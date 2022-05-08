package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink03_Project_Market {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .addSource(new MarketSource())
            .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(MarketingUserBehavior mub) throws Exception {
                    return Tuple2.of(mub.getChannel() + "_" + mub.getBehavior(), 1L);
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MarketSource implements SourceFunction<MarketingUserBehavior> {
        
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            
            String[] behaviors = {"download", "install", "update", "uninstall"};
            String[] channels = {"Appstore", "huawei", "xiaomi", "oppo", "vivo"};
            
            while (true) {
                
                Long userId = (long) random.nextInt(1000) + 1;
                String behavior = behaviors[random.nextInt(behaviors.length)];
                String channel = channels[random.nextInt(channels.length)];
                
                ctx.collect(new MarketingUserBehavior(
                    userId,
                    behavior,
                    channel,
                    System.currentTimeMillis()
                ));
                
                Thread.sleep(300);
            }
        }
        
        @Override
        public void cancel() {
        
        }
    }
}
