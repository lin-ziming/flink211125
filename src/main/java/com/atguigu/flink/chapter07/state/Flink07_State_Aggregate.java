package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/5/11 14:11
 */
public class Flink07_State_Aggregate {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
    
    
                private AggregatingState<WaterSensor, Double> avgVcState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    avgVcState = getRuntimeContext()
                        .getAggregatingState(
                            new AggregatingStateDescriptor<WaterSensor, SumCount, Double>(
                                "avgVcState",
                                new AggregateFunction<WaterSensor, SumCount, Double>() {
                                    @Override
                                    public SumCount createAccumulator() {
                                        return new SumCount();
                                    }
                
                                    @Override
                                    public SumCount add(WaterSensor value, SumCount acc) {
                                        acc.sum += value.getVc();
                                        acc.count++;
                                        return acc;
                                    }
                
                                    @Override
                                    public Double getResult(SumCount acc) {
                                        return acc.sum * 1.0 / acc.count;
                                    }
                
                                    @Override
                                    public SumCount merge(SumCount a, SumCount b) {
                                        return null;
                                    }
                                },
                                SumCount.class
                            ));
        
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    avgVcState.add(value);
                    
                    out.collect(ctx.getCurrentKey() + " 的水位和是:" + avgVcState.get());
                    
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static class SumCount{
        public Integer sum = 0;
        public Long count = 0L;
    }
}
