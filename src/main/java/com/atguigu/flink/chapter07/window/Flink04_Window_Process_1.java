package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/10 9:07
 */
public class Flink04_Window_Process_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        // 每隔5秒计算5内的水位和
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
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1,
                                              WaterSensor value2) throws Exception {
                        System.out.println("xxxxx");
                        
                        value1.setVc(value1.getVc() + value2.getVc());
                        
                        return value1;
                    }
                },
                /*new WindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,  //
                                      Iterable<WaterSensor> input,  // 有且仅有一个元素
                                      Collector<String> out) throws Exception {
                        WaterSensor ws = input.iterator().next();
    
                        out.collect(ws + "  " + window);
                    }
                }*/
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> input,
                                        Collector<String> out) throws Exception {
                        WaterSensor ws = input.iterator().next();
                        
                        
                        out.collect(ws + "  " + ctx.window());
                    }
                }
            )
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
家窗口的的目的是对窗口内的元素进行计算处理,
窗口处理函数


1. 增量聚合
    简单
        sum, max, min, maxBy, minBy
    复杂
        reduce
        
        aggregate

2. 全量聚合

 */