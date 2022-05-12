package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/11 14:11
 */
public class Flink08_StateBackend {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "atguigu"); // 大小写切换: ctrl+shift+u
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // 启动checkpoint
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        
        // 1. 内存级别
        // 1.1 旧的写法
        //        env.setStateBackend(new MemoryStateBackend());   // 默认值
        // 1.2 新的写法
//        env.setStateBackend(new HashMapStateBackend());  // 本地
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage()); // 远程在jobmanager的内存
        
        // 2.fs
        // 2.1 旧的写法
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/ck"));
        // 2.2 新的写法
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck1");
        
        // 3. rocksdb
        // 3.1 旧的写法
//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/ck2"));
        
        // 3.2 新的写法
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck3");
        
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                
                private MapState<Integer, Object> vcMapState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    vcMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Object>("vcMapState", Integer.class, Object.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    vcMapState.put(value.getVc(), new Object());
                    
                    Iterable<Integer> keys = vcMapState.keys();
                    List<Integer> list = AtguiguUtil.toList(keys);
                    
                    out.collect(ctx.getCurrentKey() + " 水位去重: " + list);
                    
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
