package cn.sysu.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author : song bei chang
 * @create 2021/11/21 00:23
 */
public class Flink04_TransForm_Filter_Lambda {

    public static void main(String[] args) throws Exception {
        anonymous();
        lambda();
    }


    public static void anonymous() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> streamOperator = env.fromElements(1, 2, 3, 4, 5).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });

        streamOperator.print("anonymous");

        env.execute();
    }

    public static void lambda() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5)
           .filter(value -> value % 2  == 0)
           .print("lambda");

        env.execute();


    }
}



