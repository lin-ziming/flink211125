package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.bean.TxEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink04_Project_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new OrderEvent(
                    Long.valueOf(data[0]),
                    data[1],
                    data[2],
                    Long.valueOf(data[3])
                );
            })
            .filter(e -> "pay".equals(e.getEventType()));
        
        
        SingleOutputStreamOperator<TxEvent> txEventStream = env
            .readTextFile("input/ReceiptLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new TxEvent(
                    data[0],
                    data[1],
                    Long.valueOf(data[2])
                );
            });
        
        
        orderEventStream.connect(txEventStream)
            //            .keyBy("txId", "txId")
            .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
            .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                
                HashMap<String, OrderEvent> orderEventMap = new HashMap<>();
                HashMap<String, TxEvent> txEventMap = new HashMap<>();
                
                @Override
                public void processElement1(OrderEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    // 入股支付信息来了, 查看交易流水是否到了,如果已经到了, 就对账成功
                    // 如果没到,把自己存起来, 然后等待交易流水到的时候进行获取
                    String txId = value.getTxId();
                    TxEvent txEvent = txEventMap.get(txId);
                    if (txEvent != null) {
                        out.collect("订单: " + value.getOrderId() + " 对账成功....");
                    } else {
                        orderEventMap.put(txId, value);
                    }
                    
                }
                
                @Override
                public void processElement2(TxEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    String txId = value.getTxId();
                    OrderEvent orderEvent = orderEventMap.get(txId);
                    if (orderEvent != null) {
                        
                        out.collect("订单: " + orderEvent.getOrderId() + " 对账成功....");
                    } else {
                        txEventMap.put(txId, value);
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
