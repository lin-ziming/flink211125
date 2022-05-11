package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/5/11 14:11
 */
public class Flink01_State_Operator_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(3000);  // checkpoint 周期
        
        
        env
            .socketTextStream("hadoop162", 9999)
            .flatMap(new MyFlatMapFunction())
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyFlatMapFunction implements FlatMapFunction<String, String>, CheckpointedFunction {
        
        ArrayList<String> list = new ArrayList<>();
        private ListState<String> wordsState;
    
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            
            if (value.contains("x")) {
                throw new RuntimeException("程序自动重启....");
            }
            
            String[] words = value.split(",");
            
            for (String word : words) {
                
                list.add(word);
            }
            
            out.collect(list.toString());
        }
        
        // 给状态做快照
        // 把要保存的数据保存到状态中
        // 周期性的执行, 次数和并行度相等
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//            System.out.println("MyFlatMapFunction.snapshotState");
           /* wordsState.clear();
            wordsState.addAll(list);*/
            wordsState.update(list);
            
        }
        
        // 初始化状态: 从状态中恢复数据
        // 重新启动的时候执行, 次数和并行度相关
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("MyFlatMapFunction.initializeState");
            wordsState = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<String>("wordsState", String.class));
            
            // 从状态中恢复数据
            Iterable<String> it = wordsState.get();
            for (String s : it) {
                list.add(s);
                
            }
        }
    }
}
