package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @Author lzc
 * @Date 2022/5/12 11:06
 */
public class Flink09_Kafka_Flink_Kafka {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //1. 创建一个流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env.enableCheckpointing(3000);  // 开启checkpoint
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck10");
        
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  //设置checkpoint的语义
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 最多同时指向一个checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // 设置两个checkpoint之间的最小间隔
        env.getCheckpointConfig().setCheckpointTimeout(50 * 1000);
        
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION); // 程序被cancel后, 外部状态是否保留
        
        
        
        Properties sourceProps = new Properties();
        sourceProps.put("bootstrap.servers", "hadoop162:9092");
        sourceProps.put("group.id", "Flink09_Kafka_Flink_Kafka2");
        sourceProps.put("auto.reset.offset", "latest");
        
        Properties sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "hadoop162:9092");
        sinkProps.put("transaction.timeout.ms", 15 * 60 * 1000);
        
        // ToDO
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = env
            .addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps))
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : value.split(",")) {
                        out.collect(Tuple2.of(word, 1L));
                        
                    }
                }
            })
            .keyBy(t -> t.f0)
            .sum(1);
        
        
        resultStream
            .addSink(
                new FlinkKafkaProducer<Tuple2<String, Long>>(
                    "s2",
                    new KafkaSerializationSchema<Tuple2<String, Long>>() {
                        @Override
                        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> t,
                                                                        @Nullable Long timestamp) {
                            return new ProducerRecord<>("s2", (t.f0 + "_" + t.f1).getBytes(StandardCharsets.UTF_8));
                        }
                    },
                    sinkProps,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE // 开启两阶段提交
                ));
    
        resultStream
            .addSink(new SinkFunction<Tuple2<String, Long>>() {
                @Override
                public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                    if (value.f0.contains("x")) {
                        throw new RuntimeException();
                    }
                }
            });
        env.execute();
        
    }
}
