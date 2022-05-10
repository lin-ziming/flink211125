package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;

/**
 * @Author lzc
 * @Date 2022/5/10 9:07
 */
public class Flink01_Window_ProcessingTime {
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
            //            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//            .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
            // 窗口处理函数
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements, // 存储了窗口内所有的元素
                                    Collector<String> out) throws Exception {
                    ArrayList<WaterSensor> list = new ArrayList<>();
                    for (WaterSensor element : elements) {
                        list.add(element);
                    
                    }
                    // [0,5)  前闭后开
                    Date stt = new Date(ctx.window().getStart());
                    Date edt = new Date(ctx.window().getEnd());
                    String msg = "窗口:" + stt + "  " + edt + "  " + list;
                    out.collect(msg);
                
                
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
