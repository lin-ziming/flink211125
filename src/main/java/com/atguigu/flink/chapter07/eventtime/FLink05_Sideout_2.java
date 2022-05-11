package com.atguigu.flink.chapter07.eventtime;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lzc
 * @Date 2022/5/10 15:12
 */
public class FLink05_Sideout_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        SingleOutputStreamOperator<WaterSensor> main = env
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
            .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                @Override
                public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                    // snsor_1存入主流 sesor_2存入测输出流
                    if (value.getId().equals("sensor_1")) {
                        out.collect(value);
                    }else{
                        ctx.output(new OutputTag<WaterSensor>("other"){}, value);
                    }
                }
            });
    
        main.print("s1");
        main.getSideOutput(new OutputTag<WaterSensor>("other"){}).print("s2");
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
