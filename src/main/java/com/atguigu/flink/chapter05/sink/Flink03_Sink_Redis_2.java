package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/5/7 15:33
 */
public class Flink03_Sink_Redis_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994001L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
    
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop162")
            .setPort(6379)
            .setMaxTotal(100)
            .setMaxIdle(10)
            .setMinIdle(2)
            .setTimeout(10 * 1000)
            .build();
        
    
        env
            .fromCollection(waterSensors)
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(new RedisSink<>(config, new RedisMapper<WaterSensor>() {
                
                // 返回redis的命令描述符
                @Override
                public RedisCommandDescription getCommandDescription() {
                    // 写入字符串
                    return new RedisCommandDescription(RedisCommand.SADD, null);
                }
    
                // 返回要写入的key
                @Override
                public String getKeyFromData(WaterSensor data) {
                    return data.getId();
                }
    
                @Override
                public String getValueFromData(WaterSensor data) {
                    return JSON.toJSONString(data);
                }
            }));
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