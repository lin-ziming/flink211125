package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/11 10:32
 */
public class Flink01_Timer_ProcessingTime {
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
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
    
                private long ts;  // 状态: 和并行度一致
    
                // 当定时器触发的时候会执行这个方法
                @Override
                public void onTimer(long timestamp,  // 定时器的定时的时间
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect(ctx.getCurrentKey() + " 发出预警: " + timestamp);
                    
                }
    
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 当水位值大于20, 注册一个5s后执行的定时器
                    
    
                    if (value.getVc() > 20) {
                        // 定时器的时间: 绝对时间
                        ts = ctx.timerService().currentProcessingTime() + 5000;
                        System.out.println("注册定时器: " + ts);
                        ctx.timerService().registerProcessingTimeTimer(ts);
                    }else if(value.getVc() < 20){
                        System.out.println("删除定时器: " + ts);
                        ctx.timerService().deleteProcessingTimeTimer(ts);
                    }
                    
        
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