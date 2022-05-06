package com.atguigu.flink.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
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
public class Flink02_UnBounded_WC_Lambda {
    public static void main(String[] args) throws Exception {
        System.out.println("xxxxxx");
        //1. 创建一个流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 通过执行环节得到一个流
//        DataStreamSource<String> source = env.readTextFile("input/words.txt");
        DataStreamSource<String> source = env.socketTextStream("hadoop162", 9999);
        
        // 3. 对流做各种转换
        SingleOutputStreamOperator<String> wordStream = source.flatMap((String line, Collector<String> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        
        }).returns(Types.STRING);
        // hello -> (hello,1)
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneStream = wordStream
            .map(word -> Tuple2.of(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));
        
        // keyBy  分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordOneStream.keyBy( t -> t.f0);
        // 流中存储的是元组, 这个地方写元组的位置  从0开始
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);
        
        // 4. 输出流数据
        result.print();
        
        
        // 5. 执行执行环境
        env.execute();
        
    }
}
/*


 */