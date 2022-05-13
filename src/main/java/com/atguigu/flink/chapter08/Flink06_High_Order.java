package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

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
    
        env
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
            .keyBy(OrderEvent::getOrderId)
            .window(EventTimeSessionWindows.withGap(Time.minutes(45)))
            .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {
            
                private ValueState<OrderEvent> createState;
            
                @Override
                public void open(Configuration parameters) throws Exception {
                    createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                }
            
                @Override
                public void process(Long key,
                                    Context ctx,
                                    Iterable<OrderEvent> elements,
                                    Collector<String> out) throws Exception {
                    List<OrderEvent> list = AtguiguUtil.toList(elements);
                
                    if (list.size() == 2) {
                        out.collect("订单: " + key + " 正常创建和支付");
                    } else {
                        OrderEvent event = list.get(0);
                        String eventType = event.getEventType();
                        if ("create".equals(eventType)) {
                            createState.update(event);
                        } else {
                            OrderEvent createEvent = createState.value();
                            if (createEvent == null) {
                                out.collect("订单: " + key + " 只有pay, 没有create, 请检查系统是否有问题...");
                            } else {
                                out.collect("订单: " + key + " 被超时支付, 请检查系统是否有问题...");
                            
                            }
                        }
                    
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
