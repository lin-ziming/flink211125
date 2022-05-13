package com.atguigu.flink.chapter10;

import com.atguigu.flink.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink06_High_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        KeyedStream<OrderEvent, Long> keyedStream = env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new OrderEvent(
                    Long.valueOf(data[0]),
                    data[1],
                    data[2],
                    Long.parseLong(data[3]) * 1000
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((order, ts) -> order.getEventTime())
            )
            .keyBy(OrderEvent::getOrderId);
        
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
            .<OrderEvent>begin("create", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "create".equals(value.getEventType());
                }
            }).optional()
            .next("pay")
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "pay".equals(value.getEventType());
                }
            })
            .within(Time.minutes(45));
        
        PatternStream<OrderEvent> ps = CEP.pattern(keyedStream, pattern);
        
        ps
            .select(new PatternSelectFunction<OrderEvent, String>() {
                @Override
                public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                    List<OrderEvent> create = pattern.get("create");
                    List<OrderEvent> pay = pattern.get("pay");
                    if (create == null) {
                        return pay.get(0).getOrderId() + " 只有pay, 没有crete";
                    } else {
                        
                        return create.get(0).getOrderId() + " 正常支付";
                    }
                    
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
