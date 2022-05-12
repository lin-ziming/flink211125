package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/5/12 15:09
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class HotItem {
    private Long itemId;
    private Long wEnd;
    private Long count;
}
