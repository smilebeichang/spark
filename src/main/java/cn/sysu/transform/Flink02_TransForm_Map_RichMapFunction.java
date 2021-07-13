package cn.sysu.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/4/18 23:01
 */
public class Flink02_TransForm_Map_RichMapFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(5);

        env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MyRichMapFunction()).setParallelism(3)
                .print();

        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<Integer,Integer>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println(" open  调用一次 ");
            //{}
            System.out.println(parameters.toString());
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            System.out.println(" close  调用一次 ");

            super.close();
        }

        @Override
        public Integer map(Integer value) throws Exception {
            System.out.println(" map  一个元素调用一次 ");

            return value * value;
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            System.out.println(super.getRuntimeContext().toString());
            return super.getRuntimeContext();
        }
    }

}



