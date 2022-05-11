package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/11 10:32
 */
public class Flink03_Timer_Practice {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env
            .socketTextStream("hadoop162", 9999)
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String value) throws Exception {
                    
                    String[] data = value.split(",");
                    return new WaterSensor(
                        data[0],
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2])
                    );
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            )
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private long timerTs;
                int lastVc = 0;
                
                // 当定时器触发的时候会执行这个方法
                @Override
                public void onTimer(long timestamp,  // 定时器的定时的时间
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect(ctx.getCurrentKey() + " 连续5s水位上升发出预警: " + timestamp);
                    lastVc = 0; // 下条数据进来, 会当作第一条数据处理
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    /*
                    当第一条数据来的时候,, 注册一个5s后执行的定时器
                    1. 如果上升, 则什么都不做
                    2. 如果下降,则删除定时器, 并注册一个新的定时器
                     */
                    if (lastVc == 0) { // 第一条进来, 没有上一个水位值
                        timerTs = value.getTs() + 5000;
                        ctx.timerService().registerEventTimeTimer(timerTs);
                        System.out.println("第一条数据进来注册定时器:" + timerTs);
                    } else {
                        if (value.getVc() < lastVc) { // 水位下降
                            ctx.timerService().deleteEventTimeTimer(timerTs);
                            timerTs = value.getTs() + 5000;
                            System.out.println("水位下降, 注册新的定时器: " + timerTs);
                            ctx.timerService().registerEventTimeTimer(timerTs);
                        } else {
                            System.out.println("水位上升或不变, 什么都不做");
                            
                        }
                    }
                    
                    lastVc = value.getVc();
                }
            })
            .print();
        
        
        env.execute();
    }
}
/*
注册定时器


删除定时器

定时器只能用在keyBy之后的流上, 定时器和key绑定在一起

 */