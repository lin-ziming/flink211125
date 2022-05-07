package com.atguigu.flink.chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
      
        env.setParallelism(2);
        env.disableOperatorChaining();
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
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneStream = wordStream
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String word) throws Exception {
                    return Tuple2.of(word, 1L);
                }
            })
            .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                    return value;
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
如何修改算子的并行度
1. 在配置中文件中指定
    parallelism.default: 1

2. 在提交job的时候, 通过 -p 参数指定
    -p 2

3. 在代码中, 通过env来指定job的所有算子的并行度
    env.setParallelism(1);
    
4. 在代码中,单独给算子设置并行度
   .setParallelism(1);

------
.startNewChain();
    当前算子不和前面的算子优化在一起
.disableChaining()
    这个算子单独
 env.disableOperatorChaining();
    全局禁用
  
.
 */