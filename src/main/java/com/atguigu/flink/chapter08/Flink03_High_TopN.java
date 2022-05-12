package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.HotItem;
import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/8 13:46
 */
public class Flink03_High_TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]),
                    data[3],
                    Long.parseLong(data[4]) * 1000
                );
            })
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ub, ts) -> ub.getTimestamp())
            )
            .keyBy(UserBehavior::getItemId)
            .window(TumblingEventTimeWindows.of(Time.hours(3)))
            .aggregate(
                new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                
                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
                        return accumulator + 1;
                    }
                
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }
                
                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context ctx,
                                        Iterable<Long> elements,
                                        Collector<HotItem> out) throws Exception {
                        HotItem hotItem = new HotItem(key, ctx.window().getEnd(), elements.iterator().next());
                        out.collect(hotItem);
                    }
                }
            )
            .keyBy(HotItem::getWEnd)
            .process(new KeyedProcessFunction<Long, HotItem, String>() {
    
                private ListState<HotItem> hotItemState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    hotItemState = getRuntimeContext()
                        .getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    
                }
    
                @Override
                public void processElement(HotItem hotItem,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 第一条数据进来的时候注册定时器: 窗口结束时间+5s
                    // 一个状态, 存储所有的HotItem
                    Iterable<HotItem> hotItemIt = hotItemState.get();
                    Iterator<HotItem> it = hotItemIt.iterator();
                    if (!it.hasNext()) {  // 如果是空表示第一个元素
                        // 注册定时器
                        ctx.timerService().registerEventTimeTimer(hotItem.getWEnd() + 2000);
                        
                    }
                    hotItemState.add(hotItem);
                    
                }
    
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    List<HotItem> hotItems = AtguiguUtil.toList(hotItemState.get());
                    
                    hotItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
    
                    String msg = "---------------\n";
                    for (int i = 0; i < Math.min(4, hotItems.size()); i++) {
                        msg += hotItems.get(i) + "\n";
                    }
                    
                    out.collect(msg);
                    
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
