import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * @Author lzc
 * @Date 2022/5/6 11:18
 */
public class LambdaDemo2 {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("abc");
        list.add("hello");
        list.add("atguigu");
        list.add("abc");
        
        // 得到一个新的集合: 存储的是每个元素的长度  3,5,6,3
       /* List<Integer> list1 = list.stream().map(String::length).collect(Collectors.toList());
        System.out.println(list1);*/
    
        System.out.println(list.stream().filter(x -> x.length() >= 5).collect(Collectors.toList()));
    }
}
