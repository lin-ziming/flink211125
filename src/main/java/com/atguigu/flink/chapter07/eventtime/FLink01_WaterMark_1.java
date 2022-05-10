package com.atguigu.flink.chapter07.eventtime;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/10 15:12
 */
public class FLink01_WaterMark_1 {
    public static void main(String[] args) {
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
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        // 返回事件时间: 必须是一个毫秒值
                        // 水印在流中一般只加一次, recordTimestamp就是long的最小值
                        @Override
                        public long extractTimestamp(WaterSensor element,
                                                     long recordTimestamp) {
                            return element.getTs();
                        }
                    })
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    List<WaterSensor> list = AtguiguUtil.toList(elements);
                
                    out.collect(ctx.window() + "   " + list);
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
