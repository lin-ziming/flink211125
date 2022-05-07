package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/5/7 13:47
 */
public class Flink04_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        env
            .fromElements(1, 2, 3, 4, 5)
           /* .keyBy(new KeySelector<Integer, Integer>() {
                @Override
                public Integer getKey(Integer value) throws Exception {
                    return value % 2;
                }
            })*/
           .keyBy(new KeySelector<Integer, String>() {
               @Override
               public String getKey(Integer value) throws Exception {
                   return value % 2 == 0 ? "偶数" : "奇数";
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
/*


 new KeyGroupStreamPartitioner<>(
                                keySelector,
                                StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM))

128


// 0或1, 128, 2
KeyGroupRangeAssignment.assignKeyToParallelOperator(
                key, maxParallelism, numberOfChannels)   = 如果 <64  返回0 如果>=64 1


// 128, 2, [0,128)
computeOperatorIndexForKeyGroup(
                maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism))


      key, 128
      assignToKeyGroup(key, maxParallelism)  [0,128)

            key.hashCode, 128
            computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);  [0,128)

                // [0,128)
                MathUtils.murmurHash(keyHash) % maxParallelism;



 */
