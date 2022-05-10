package com.atguigu.flink.chapter07.eventtime;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/10 15:12
 */
public class FLink02_WaterMark_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env.getConfig().setAutoWatermarkInterval(1000);
        
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
                new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(
                        WatermarkGeneratorSupplier.Context context) {
                        return new MyGenerator();
                    }
                }
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
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
    
    // 自定义水印生成器
    public static class MyGenerator implements WatermarkGenerator<WaterSensor> {
        
        long maxTs = Long.MIN_VALUE + 2000;
        
        // 每来一个元素执行一次
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("MyGenerator.onEvent");
            
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - 2000));
            
        }
        
        // 周期性的执行: 默认200ms执行一次
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyGenerator.onPeriodicEmit");
            //            output.emitWatermark(new Watermark(maxTs - 2000));
            
        }
    }
}
