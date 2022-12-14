package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/11 14:11
 */
public class Flink05_State_List {
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
    
    
                private ListState<Integer> top3VcState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    top3VcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Integer.class));
                }
    
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    top3VcState.add(value.getVc());
    
                    Iterable<Integer> it = top3VcState.get();
                    List<Integer> list = AtguiguUtil.toList(it);
                    
//                    list.sort((o1, o2) -> o2.compareTo(o1));
                    list.sort(Comparator.reverseOrder());
    
                    if (list.size() == 4) {
                        // ?????????????????????
                        list.remove(3);
                    }
    
                    top3VcState.update(list);
                    
                    out.collect(ctx.getCurrentKey() + " top3: " + list);
    
    
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
