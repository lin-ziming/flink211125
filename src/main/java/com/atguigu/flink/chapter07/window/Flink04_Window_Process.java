package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author lzc
 * @Date 2022/5/10 9:07
 */
public class Flink04_Window_Process {
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
            //            .sum("vc")
            /*.reduce(new ReduceFunction<WaterSensor>() {
                @Override
                public WaterSensor reduce(WaterSensor value1,
                                          WaterSensor value2) throws Exception {
                    System.out.println("xxxxx");
    
                    value1.setVc(value1.getVc() + value2.getVc());
                    
                    return value1;
                }
            })*/
            
            .aggregate(
                new AggregateFunction<WaterSensor, Tuple2<Integer, Long>, Double>() {
                    // 初始化一个累加器
                    @Override
                    public Tuple2<Integer, Long> createAccumulator() {
                        System.out.println("Flink04_Window_Process.createAccumulator");
                        return Tuple2.of(0, 0L);
                    }
                    
                    // 聚合操作
                    @Override
                    public Tuple2<Integer, Long> add(WaterSensor value,
                                                     Tuple2<Integer, Long> acc) {
                        System.out.println("Flink04_Window_Process.add");
                        return Tuple2.of(acc.f0 + value.getVc(), acc.f1 + 1);
                    }
                    
                    // 返回最终的聚合结果
                    @Override
                    public Double getResult(Tuple2<Integer, Long> acc) {
                        System.out.println("Flink04_Window_Process.getResult");
                        return acc.f0 * 1.0 / acc.f1;
                    }
                    
                    // 合并累累加器的值  这个方法只有当窗口是session窗口才会生效, 其他窗口不执行
                    @Override
                    public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
                        System.out.println("Flink04_Window_Process.merge");
                        return null;
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