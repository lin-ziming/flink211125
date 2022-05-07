package com.atguigu.flink.chapter05.source;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author lzc
 * @Date 2022/5/7 11:30
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        /// 自定义socket source
        DataStreamSource<WaterSensor> stream = env.addSource(new MySocketSource());
        stream.print();
        
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public static class MySocketSource implements SourceFunction<WaterSensor>{
    
        // 核心方法: 读取数据
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // 从socket读取数据
            Socket socket = new Socket("hadoop162", 9999);
            InputStream is = socket.getInputStream();
            InputStreamReader isReader = new InputStreamReader(is, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isReader);
    
            String line = reader.readLine();
    
            while (line != null) {
                String[] data = line.split(",");
                ctx.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));
    
                line = reader.readLine();
            }
    
    
        }
        // 取消source, 给外界调用用来停止source
        @Override
        public void cancel() {
           // System.exit(0);
           
        }
    }
}
