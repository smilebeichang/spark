package cn.sysu.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/20 22:48
 */
public class Flink01_TransForm_Map_Anonymous {

    public static void main(String[] args) throws Exception {
        anonymous();
        lambda();
        staticClass();

    }

    private static void anonymous() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> streamOperator = env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MapFunction<Integer, Integer>() {

                    @Override
                    public Integer map(Integer value) throws Exception {

                        return value * value;
                    }
                });
        streamOperator.print();
        env.execute();

    }


    private static void lambda() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5)
                .map(ele -> ele * ele)
                .print();
        env.execute();
    }


    private static void staticClass() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MyMapFunction())
                .print();

        env.execute();
    }


    private static class MyMapFunction implements MapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) throws Exception {
            return value * value;
        }
    }


}






