package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/11 15:02
 */
public class Flink03_State_BC {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 8888);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop162", 9999);
        MapStateDescriptor<String, String> bcStateDesc = new MapStateDescriptor<>("bcState", String.class, String.class);
        // 1. 把控制流做成广播流
        BroadcastStream<String> bcState = controlStream.broadcast(bcStateDesc);
        // 2. 让数据流connect 广播流
        BroadcastConnectedStream<String, String> stream = dataStream.connect(bcState);
        
        
        stream
            .process(new BroadcastProcessFunction<String, String, String>() {
                
                // 处理数据流中的元素
                @Override
                public void processElement(String value,
                                           ReadOnlyContext ctx,
                                           Collector<String> out) throws Exception {
                    // 4. 数据流中数据, 从广播状态读取配置信息
                    ReadOnlyBroadcastState<String, String> bcState = ctx.getBroadcastState(bcStateDesc);
                    String aSwitch = bcState.get("swtich");
                    
                    if ("1".equals(aSwitch)) {
                        out.collect("切换到1号处理逻辑...");
                    } else if ("2".equals(aSwitch)) {
                        
                        out.collect("切换到2号处理逻辑...");
                    } else {
                        out.collect("切换到默认处理逻辑...");
                        
                    }
                }
                
                // 处理广播流中的元素
                @Override
                public void processBroadcastElement(String value,
                                                    Context ctx,
                                                    Collector<String> out) throws Exception {
                   // 3. 把配置信息放入广播状态
                    BroadcastState<String, String> bcState = ctx.getBroadcastState(bcStateDesc);
    
                    // 输入1 , 2
                    bcState.put("swtich", value);
    
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
