package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.AdsClickLog;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink04_High_AdsClick {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        SingleOutputStreamOperator<String> normal = env
            .readTextFile("input/AdClickLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new AdsClickLog(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    data[2],
                    data[3],
                    Long.parseLong(data[4]) * 1000
                );
            })
            .keyBy(log -> log.getUserId() + "_" + log.getAdsId())
            .process(new KeyedProcessFunction<String, AdsClickLog, String>() {
                
                private ValueState<String> yesterdayState;
                private ValueState<Boolean> alreadyBlackListState;
                private ReducingState<Long> clickCountState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    clickCountState = getRuntimeContext()
                        .getReducingState(new ReducingStateDescriptor<Long>("clickCountState", Long::sum, Long.class));
                    
                    alreadyBlackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("alreadyBlackListState", Boolean.class));
                    
                    yesterdayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("yesterdayState", String.class));
                }
                
                @Override
                public void processElement(AdsClickLog value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    // 如果判断跨天
                    // 使用一个状态记录昨天的日志, 每来一条数据就与这个日期进行比较, 如果相等, 证明还在同一天, 如果不等, 跨天.
                    String yesterday = yesterdayState.value();
                    String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.getTimestamp()));
                    if (!today.equals(yesterday)) {
                        
                        clickCountState.clear();
                        alreadyBlackListState.clear();
                        yesterdayState.update(today);
                        
                        if(value.getUserId() == 937166 && value.getAdsId() == 1715){
                            System.out.println(today);
                        }
                        
                    }
    
    
                    if (alreadyBlackListState.value() == null) {
                        
                        clickCountState.add(1L);
                    }
                    
                    
                    Long clickCount = clickCountState.get();
                    
                    String msg = value.getUserId() + " 对广告: " + value.getAdsId() + "的点击量是: " + clickCount;
                    if (clickCount > 99) {
                        if (alreadyBlackListState.value() == null) {
                            
                            ctx.output(new OutputTag<String>("blackList") {}, msg + " 超过阈值, 加入黑名单");
                            alreadyBlackListState.update(true);
                            
                        }
                        
                    } else {
                        out.collect(msg);
                    }
                }
            });
        
        normal.print("normal");
        normal.getSideOutput(new OutputTag<String>("blackList") {}).print("black");
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
