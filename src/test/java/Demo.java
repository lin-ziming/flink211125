import org.apache.flink.util.MathUtils;

/**
 * @Author lzc
 * @Date 2022/5/7 14:40
 */
public class Demo {
    public static void main(String[] args) {
//        Integer a = 0;
//        Integer b = 1;
    
        String a = "偶数";
        String b = "奇数";
    
        System.out.println(MathUtils.murmurHash(a.hashCode()) % 128);
        System.out.println(MathUtils.murmurHash(b.hashCode()) % 128);
    }
}
