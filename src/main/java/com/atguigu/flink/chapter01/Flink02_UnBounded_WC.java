package com.atguigu.flink.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/6 10:09
 */
public class Flink02_UnBounded_WC {
    public static void main(String[] args) throws Exception {
        //1. 创建一个流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 通过执行环节得到一个流
//        DataStreamSource<String> source = env.readTextFile("input/words.txt");
        DataStreamSource<String> source = env.socketTextStream("hadoop162", 9999);
        
        // 3. 对流做各种转换
        SingleOutputStreamOperator<String> wordStream = source.flatMap(new FlatMapFunction<String, String>() {
        
            @Override
            public void flatMap(String line,
                                Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            
            }
        });
        // hello -> (hello,1)
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneStream = wordStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
        
        // keyBy  分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordOneStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> t) throws Exception {
                return t.f0;
            }
        });
        // 流中存储的是元组, 这个地方写元组的位置  从0开始
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);
        
        // 4. 输出流数据
        result.print();
        
        
        // 5. 执行执行环境
        env.execute();
        
    }
}
/*
spark:
    1. 创建sparkContext
    
    2. 通过上下文获取一个rdd
    
    3. 对rdd做各种转换
    
    4. 行动算子
    
    5. 启动上下文


 */