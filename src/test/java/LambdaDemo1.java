/**
 * @Author lzc
 * @Date 2022/5/6 10:53
 */
public class LambdaDemo1 {
    
    public static void main(String[] args) {
        /*f1(new A() {
            @Override
            public void f1() {
                System.out.println("我是匿名内部类的方法....");
            }
        });*/
        //test1(() -> System.out.println("我是拉姆达表达式的方法...."));
        
        //        test2(user ->  user.getName());
//        test2(User::getName);
        
        
//        test3(User::new);
        A a = () -> System.out.println("xxx");
        
        a.f1();
        
    }
    
    public static void test3(C c){
        System.out.println(c.f1());
    }
    
    public static void test2(B b) {
        b.f1(new User());
    }
    
    public static void test1(A a) {
        a.f1();
    }
    
    
}
interface C{
    User f1();
}



class User {
    private String name;
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
}

interface B {
    String f1(User user);
}

interface A {
    void f1();
    
    //    void f2();
    default void f2() {
        System.out.println("xxxx");
    }
}


/*
 函数式接口
    如果一个接口只有一个抽象方法
 
 
 User::getName 方法引用
 
 User::new
*/