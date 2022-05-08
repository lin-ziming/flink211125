package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/5/7 15:33
 */
public class Flink05_Sink_Cumstom_1 {
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
            .addSink(new MySqlSink());
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MySqlSink extends RichSinkFunction<WaterSensor> {
    
        private Connection connection;
    
        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.jdbc.Driver");
            // alt+ctrl+f 提升为成员变量
            connection = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test?useSSL=false", "root", "aaaaaa");
        }
    
        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }
    
        //数据写出
        @Override
        public void invoke(WaterSensor value,
                           Context ctx) throws Exception {
            // insert into sensor(id, ts, vc)values(?,?,?) on duplicate key update vc=?
//            String sql = "insert into sensor(id, ts, vc)values(?,?,?) on duplicate key update vc=?";
            String sql = "replace into sensor(id, ts, vc)values(?,?,?)";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            ps.execute();
            ps.close();
            
        }
    }
}
