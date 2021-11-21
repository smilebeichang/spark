package cn.sysu.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : song bei chang
 * @create 2021/11/20 11:06
 */
public class Sout {

    public static void main(String[] args) {
        System.out.println(new Person("xiao","shen"));
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Person{
    String name ;
    String address ;

}



