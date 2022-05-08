package com.atguigu.flink.chapter05.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/5/8 9:14
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop162:9092");
        env
            .socketTextStream("hadoop162", 9999)
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : line.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                    
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .map(t -> t.f0 + "_" + t.f1)
            .addSink(new FlinkKafkaProducer<String>("s2", new SimpleStringSchema(), props));
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
