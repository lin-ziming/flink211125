package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/7 15:33
 */
public class Flink04_Sink_es_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        List<HttpHost> httpHosts = Arrays.asList(
            new HttpHost("hadoop162", 9200),
            new HttpHost("hadoop163", 9200),
            new HttpHost("hadoop164", 9200)
        );
        
        ElasticsearchSink.Builder<WaterSensor> esSinkBuilder =
            new ElasticsearchSink.Builder<WaterSensor>(
                httpHosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element,  // 要写出的数据
                                        RuntimeContext ctx,  // 运行时上下文
                                        RequestIndexer indexer) {
    
                        IndexRequest index = Requests.indexRequest()
                            .index("sensor")
                            .type("_doc")
                            .id(element.getId())
                            .source(JSON.toJSONString(element), XContentType.JSON);
    
                        indexer.add(index);
                        
                    }
                }
            );
        esSinkBuilder.setBulkFlushInterval(1000);
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setBulkFlushMaxSizeMb(1);
        
        env
           .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            })
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(esSinkBuilder.build());
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
string
    set a b

list
    lpush rpush

set
    sadd
 
hash
    hset

zset
 
 */