package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink02_High_Project_Uv {
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
                    Long.parseLong(data[4]) * 1000
                );
            })
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ub, ts) -> ub.getTimestamp())
            )
            .keyBy(UserBehavior::getBehavior)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
            
                private MapState<Long, Object> userIdState;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    userIdState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Object>("userIdState", Long.class, Object.class));
                }
            
                @Override
                public void process(String s,
                                    Context ctx,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
                    userIdState.clear();
                    for (UserBehavior element : elements) {
                        userIdState.put(element.getUserId(), new Object());
                    
                    }
                
                    int uv = AtguiguUtil.toList(userIdState.keys()).size();
                
                    Date stt = new Date(ctx.window().getStart());
                    Date edt = new Date(ctx.window().getEnd());
                
                
                    out.collect(stt + "  " + edt + "  " + uv);
                
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