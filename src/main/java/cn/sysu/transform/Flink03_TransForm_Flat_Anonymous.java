package cn.sysu.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.EventListener;

/**
 * @Author : song bei chang
 * @create 2021/4/18 23:11
 */
public class Flink03_TransForm_Flat_Anonymous {

    public static void main(String[] args) throws Exception {

//        anonymous();
        lambda();

    }

    public static void anonymous() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> streamOperator = env.fromElements(1, 2, 3, 4, 5).flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                out.collect(value * value);
                out.collect(value * value * value);
            }
        });

        streamOperator.print();

        env.execute();

    }


    public static void lambda() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5).flatMap((Integer value,Collector<Integer> out) -> {
            out.collect(value * value);
            out.collect(value * value * value);
        }).returns(Types.INT).print();

        env.execute();


    }

}



