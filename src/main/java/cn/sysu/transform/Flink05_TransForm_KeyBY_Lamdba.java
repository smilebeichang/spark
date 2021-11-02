package cn.sysu.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @Author : song bei chang
 * @create 2021/4/18 23:37
 */
public class Flink05_TransForm_KeyBY_Lamdba {

    public static void main(String[] args) throws Exception {
        anonymous();
//        lambda();
    }


    public static void anonymous() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

       env.fromElements(1, 2, 3, 4, 5)
          .keyBy(new KeySelector<Integer,String>() {

            // 按照key的hash值, 双重hash
            @Override
            public String getKey(Integer value) throws Exception {
                //return value % 2 == 0 ? "偶数" : "奇数";
                return value % 2 == 0 ? "南山" : "宝安";
            }
        })

        .print();

        env.execute();
    }

    public static void lambda() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5)
           .keyBy(value -> value % 2  == 0 ? "偶数" : "奇数")
           .print();

        env.execute();


    }

}



