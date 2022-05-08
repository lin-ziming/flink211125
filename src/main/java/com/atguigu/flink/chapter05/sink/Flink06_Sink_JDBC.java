package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/5/7 15:33
 */
public class Flink06_Sink_JDBC {
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
        
        
        env
            .fromCollection(waterSensors)
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(JdbcSink.sink("replace into sensor(id, ts, vc)values(?,?,?)",
                                   new JdbcStatementBuilder<WaterSensor>() {
                                       @Override
                                       public void accept(PreparedStatement ps,
                                                          WaterSensor waterSensor) throws SQLException {
                                           ps.setString(1, waterSensor.getId());
                                           ps.setLong(2, waterSensor.getTs());
                                           ps.setInt(3, waterSensor.getVc());
                                       }
                                   },
                                   new JdbcExecutionOptions.Builder()
                                       .withBatchSize(10 * 1024)
                                       .withMaxRetries(3)
                                       .withBatchIntervalMs(1000)
                                       .build(),
                                   new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                       .withDriverName("com.mysql.jdbc.Driver")
                                       .withUrl("jdbc:mysql://hadoop162:3306/test?useSSL=false")
                                       .withUsername("root")
                                       .withPassword("aaaaaa")
                                       .build()
            ));
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
