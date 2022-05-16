package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/5/16 10:46
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordLen {
    private String word;
    private Integer len;
}
