package com.atguigu.flink.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/5/10 15:23
 */
public class AtguiguUtil {
    public static <T>List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        it.forEach(list::add);
        return list;
    }
}
